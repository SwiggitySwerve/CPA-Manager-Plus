package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The live service logged SQLITE_BUSY from its background writers whenever one write
// transaction held the lock longer than busy_timeout (5 s), for example while disk I/O was
// saturated: every other pool writer gave up instead of waiting. Writers on the pool must
// wait their turn, readers must not wait at all, and a writer's wait ends with its context.
func TestPoolWritersWaitForALongWriteInsteadOfFailingBusy(t *testing.T) {
	if testing.Short() {
		t.Skip("holds a write transaction past busy_timeout")
	}
	db, err := Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `create table gate_probe (writer text not null)`); err != nil {
		t.Fatal(err)
	}

	holder, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, `insert into gate_probe(writer) values ('holder')`); err != nil {
		t.Fatal(err)
	}
	const hold = 5500 * time.Millisecond // longer than busy_timeout(5000)
	released := time.Now().Add(hold)
	go func() {
		time.Sleep(hold)
		_ = holder.Commit()
	}()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			errs <- err
			return
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `insert into gate_probe(writer) values ('transaction')`); err != nil {
			errs <- err
			return
		}
		errs <- tx.Commit()
	}()
	go func() {
		defer wg.Done()
		_, err := db.ExecContext(ctx, `insert into gate_probe(writer) values ('statement')`)
		errs <- err
	}()

	// A reader is not queued behind the writers.
	time.Sleep(100 * time.Millisecond)
	readStarted := time.Now()
	var count int
	if err := db.QueryRowContext(ctx, `select count(*) from gate_probe`).Scan(&count); err != nil {
		t.Fatalf("read during the long write: %v", err)
	}
	if waited := time.Since(readStarted); waited > time.Second {
		t.Fatalf("read waited %s behind the writers", waited)
	}

	// A writer whose context ends while it waits gets its context error, not BUSY.
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := db.ExecContext(shortCtx, `insert into gate_probe(writer) values ('impatient')`); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer with a short deadline = %v, want context deadline exceeded", err)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("writer behind a %s write failed: %v", hold, err)
		}
	}
	if time.Now().Before(released) {
		t.Fatal("writers finished before the long write committed")
	}
	if err := db.QueryRowContext(ctx, `select count(*) from gate_probe`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("rows = %d, want holder, transaction and statement", count)
	}
}

// Transactions sent as SQL on a raw connection: a plain BEGIN (the full export's read
// snapshot) must not hold writers back, and the ROLLBACK of a BEGIN IMMEDIATE must not
// queue behind a pool writer that is waiting for it.
func TestPoolWriteGateTracksTransactionStatements(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `create table gate_probe (writer text not null)`); err != nil {
		t.Fatal(err)
	}
	reader, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.ExecContext(ctx, `begin`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := reader.QueryRowContext(ctx, `select count(*) from gate_probe`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := db.ExecContext(writeCtx, `insert into gate_probe(writer) values ('during read')`); err != nil {
		t.Fatalf("write during a deferred read transaction: %v", err)
	}
	if _, err := reader.ExecContext(ctx, `commit`); err != nil {
		t.Fatal(err)
	}

	locker, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	if _, err := locker.ExecContext(ctx, `begin immediate`); err != nil {
		t.Fatal(err)
	}
	writerDone := make(chan error, 1)
	go func() {
		tx, err := db.BeginTx(ctx, nil)
		if err == nil {
			err = tx.Commit()
		}
		writerDone <- err
	}()
	time.Sleep(200 * time.Millisecond)
	rollbackCtx, cancelRollback := context.WithTimeout(ctx, 2*time.Second)
	defer cancelRollback()
	if _, err := locker.ExecContext(rollbackCtx, `rollback`); err != nil {
		t.Fatalf("rollback of the manual transaction: %v", err)
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("writer after the manual transaction: %v", err)
	}
}

// A goroutine that holds the gate and writes through the pool again cannot be served by
// the queue. After writeGateMaxWait it reaches SQLite and fails with SQLITE_BUSY, as it did
// before the gate, instead of hanging.
func TestPoolWriteGateWaitIsBounded(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	previous := writeGateMaxWait
	writeGateMaxWait = 100 * time.Millisecond
	t.Cleanup(func() { writeGateMaxWait = previous })
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `create table gate_probe (writer text not null)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `insert into gate_probe(writer) values ('holder')`); err != nil {
		t.Fatal(err)
	}
	nestedCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err = db.ExecContext(nestedCtx, `insert into gate_probe(writer) values ('nested')`)
	if err == nil || !strings.Contains(err.Error(), "SQLITE_BUSY") {
		t.Fatalf("nested write while holding the gate = %v, want SQLITE_BUSY", err)
	}
}
