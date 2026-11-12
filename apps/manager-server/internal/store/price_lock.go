package store

import (
	"context"
	"sync"
)

// priceLock is a readers-writer lock whose waits end when the caller's context
// does. A waiting writer blocks new readers, as sync.RWMutex does, so a steady
// stream of analytics reads cannot starve a price save.
type priceLock struct {
	mu             sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	changed        chan struct{} // closed and replaced whenever the state above changes
}

func (l *priceLock) rLock(ctx context.Context) error {
	l.mu.Lock()
	for l.writer || l.waitingWriters > 0 {
		if err := l.waitLocked(ctx); err != nil {
			l.mu.Unlock()
			return err
		}
	}
	l.readers++
	l.mu.Unlock()
	return nil
}

func (l *priceLock) rUnlock() {
	l.mu.Lock()
	l.readers--
	l.notifyLocked()
	l.mu.Unlock()
}

func (l *priceLock) lock(ctx context.Context) error {
	l.mu.Lock()
	l.waitingWriters++
	for l.writer || l.readers > 0 {
		if err := l.waitLocked(ctx); err != nil {
			l.waitingWriters--
			l.notifyLocked()
			l.mu.Unlock()
			return err
		}
	}
	l.waitingWriters--
	l.writer = true
	l.mu.Unlock()
	return nil
}

func (l *priceLock) unlock() {
	l.mu.Lock()
	l.writer = false
	l.notifyLocked()
	l.mu.Unlock()
}

// waitLocked releases l.mu until the state changes or ctx ends, then retakes it.
func (l *priceLock) waitLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.changed == nil {
		l.changed = make(chan struct{})
	}
	changed := l.changed
	l.mu.Unlock()
	select {
	case <-changed:
		l.mu.Lock()
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		return ctx.Err()
	}
}

func (l *priceLock) notifyLocked() {
	if l.changed != nil {
		close(l.changed)
		l.changed = nil
	}
}
