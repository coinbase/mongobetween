package mongo

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type serverContextMap struct {
	ctxs map[address.Address]context.Context
	mu   sync.RWMutex
}

func newServerContextMap() serverContextMap {
	return serverContextMap{
		ctxs: make(map[address.Address]context.Context),
	}
}

// provides a shared context for the given server, which is canceled if the server description kind is updated to "unknown"
func (m *serverContextMap) get(parent context.Context, server *topology.SelectedServer) (context.Context, error) {
	addr := server.Description().Addr

	readWithLock := func() context.Context {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.ctxs[addr]
	}

	ctx := readWithLock()
	if ctx != nil {
		return ctx, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// check again after locking
	ctx = m.ctxs[addr]
	if ctx != nil {
		return ctx, nil
	}

	sub, err := server.Subscribe()
	if err != nil {
		return nil, err
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(parent)
	m.ctxs[addr] = ctx

	go func() {
		for desc := range sub.C {
			if desc.Kind == description.Unknown {
				break
			}
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		_ = sub.Unsubscribe()
		delete(m.ctxs, addr)
		cancel()
	}()

	return ctx, nil
}
