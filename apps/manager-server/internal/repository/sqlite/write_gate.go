package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"time"
)

// SQLite has one writer at a time. Left to SQLite, every pool connection that wants to
// write polls the lock with backoff until busy_timeout expires, in no particular order, so
// a writer that commits and immediately begins again can starve the others past the
// timeout and they fail with SQLITE_BUSY. The write gate queues the pool's writers in Go
// instead: a write transaction, or a statement run outside one, takes the gate first and
// waits for it as long as its context allows. Read-only transactions and queries never
// take it, so analytics readers are not held up. busy_timeout still covers other
// processes and connections outside the pool.
type writeGateConnector struct {
	dsn    string
	driver driver.Driver
	gate   chan struct{}
}

func openWriteGated(dsn string) (*sql.DB, error) {
	// The registered driver; sql.Open only records the name and DSN.
	probe, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	drv := probe.Driver()
	if err := probe.Close(); err != nil {
		return nil, err
	}
	return sql.OpenDB(&writeGateConnector{dsn: dsn, driver: drv, gate: make(chan struct{}, 1)}), nil
}

func (c *writeGateConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &writeGateConn{conn: conn, gate: c.gate}, nil
}

func (c *writeGateConnector) Driver() driver.Driver { return c.driver }

// sqliteConn is the part of the modernc connection the pool uses.
type sqliteConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
	driver.Validator
}

type writeGateConn struct {
	conn driver.Conn
	gate chan struct{}
	// inTx is set while this connection holds a transaction, begun through BeginTx or
	// with a BEGIN statement. database/sql uses a connection from one goroutine at a
	// time, so it needs no lock.
	inTx bool
}

// writeGateMaxWait bounds the wait in the queue. A writer still queued after it goes on to
// SQLite, whose busy_timeout then applies as it did before the gate: a goroutine that
// holds the gate and writes through the pool again fails with SQLITE_BUSY instead of
// hanging.
var writeGateMaxWait = 2 * time.Minute

func (c *writeGateConn) inner() sqliteConn { return c.conn.(sqliteConn) }

// acquire waits for the gate. It reports whether it took the gate, which the caller must
// then release; it returns false without error when the wait reached writeGateMaxWait.
func (c *writeGateConn) acquire(ctx context.Context) (bool, error) {
	select {
	case c.gate <- struct{}{}:
		return true, nil
	default:
	}
	timer := time.NewTimer(writeGateMaxWait)
	defer timer.Stop()
	select {
	case c.gate <- struct{}{}:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
		return false, nil
	}
}

func (c *writeGateConn) release() { <-c.gate }

func (c *writeGateConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	gated := false
	if !opts.ReadOnly {
		var err error
		if gated, err = c.acquire(ctx); err != nil {
			return nil, err
		}
	}
	tx, err := c.inner().BeginTx(ctx, opts)
	if err != nil {
		if gated {
			c.release()
		}
		return nil, err
	}
	c.inTx = true
	return &writeGateTx{tx: tx, conn: c, gated: gated}, nil
}

func (c *writeGateConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// ExecContext runs a statement outside a transaction under the gate, since it may write,
// and runs it as is inside one. A transaction sent as SQL on a raw sql.Conn ("begin" ...
// "commit") is tracked but takes no gate: the pool has none in production except the full
// export's read snapshot, and a BEGIN IMMEDIATE sent this way waits in SQLite like a
// writer from another process.
func (c *writeGateConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch transactionStatement(query) {
	case "begin":
		if !c.inTx {
			result, err := c.inner().ExecContext(ctx, query, args)
			if err == nil {
				c.inTx = true
			}
			return result, err
		}
	case "end":
		if c.inTx {
			result, err := c.inner().ExecContext(ctx, query, args)
			if err == nil {
				c.inTx = false
			}
			return result, err
		}
	}
	if c.inTx {
		return c.inner().ExecContext(ctx, query, args)
	}
	gated, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	if gated {
		defer c.release()
	}
	return c.inner().ExecContext(ctx, query, args)
}

// transactionStatement classifies a statement that starts or ends a transaction.
func transactionStatement(query string) string {
	words := strings.Fields(strings.ToLower(strings.TrimRight(strings.TrimSpace(query), ";")))
	if len(words) == 0 {
		return ""
	}
	switch words[0] {
	case "begin":
		return "begin"
	case "commit", "end":
		return "end"
	case "rollback":
		// ROLLBACK TO a savepoint keeps the transaction open.
		if len(words) > 1 && (words[1] == "to" || len(words) > 2 && words[2] == "to") {
			return ""
		}
		return "end"
	}
	return ""
}

func (c *writeGateConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.inner().QueryContext(ctx, query, args)
}

func (c *writeGateConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.inner().PrepareContext(ctx, query)
}

func (c *writeGateConn) Prepare(query string) (driver.Stmt, error) {
	return c.inner().Prepare(query)
}

func (c *writeGateConn) Ping(ctx context.Context) error { return c.inner().Ping(ctx) }

func (c *writeGateConn) ResetSession(ctx context.Context) error {
	return c.inner().ResetSession(ctx)
}

func (c *writeGateConn) IsValid() bool { return c.inner().IsValid() }

func (c *writeGateConn) Close() error { return c.conn.Close() }

type writeGateTx struct {
	tx    driver.Tx
	conn  *writeGateConn
	gated bool
	done  bool
}

func (t *writeGateTx) Commit() error {
	err := t.tx.Commit()
	t.finish()
	return err
}

func (t *writeGateTx) Rollback() error {
	err := t.tx.Rollback()
	t.finish()
	return err
}

func (t *writeGateTx) finish() {
	if t.done {
		return
	}
	t.done = true
	t.conn.inTx = false
	if t.gated {
		t.conn.release()
	}
}
