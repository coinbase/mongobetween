package mongo

import (
	b64 "encoding/base64"
	"github.com/coinbase/mongobetween/lruttl"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"time"
)

// on a 64-bit machine, 1 million cursors uses around 480mb of memory
const maxTransactions = 1024 * 1024

// 120 seconds default
const transactionExpiry = 120 * time.Second

type transactionCache struct {
	c *lruttl.Cache
}

func newTransactionCache() *transactionCache {
	return &transactionCache{c: lruttl.New(maxTransactions, transactionExpiry)}
}

func (t *transactionCache) count() int {
	return t.c.Len()
}

func (t *transactionCache) peek(lsID []byte) (server driver.Server, ok bool) {
	v, ok := t.c.Peek(b64.StdEncoding.EncodeToString(lsID))
	if !ok {
		return
	}
	return v.(driver.Server), true
}

func (t *transactionCache) add(lsID []byte, server driver.Server) {
	t.c.Add(b64.StdEncoding.EncodeToString(lsID), server)
}

func (t *transactionCache) remove(lsID []byte) {
	t.c.Remove(b64.StdEncoding.EncodeToString(lsID))
}
