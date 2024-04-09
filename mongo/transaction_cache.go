package mongo

import (
	b64 "encoding/base64"
	"time"

	"github.com/coinbase/mongobetween/lruttl"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
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

func (t *transactionCache) peek(lsID []byte) (conn driver.Connection, ok bool) {
	v, ok := t.c.Peek(b64.StdEncoding.EncodeToString(lsID))
	if !ok {
		return
	}
	return v.(driver.Connection), true
}

func (t *transactionCache) add(lsID []byte, conn driver.Connection) {
	t.c.Add(b64.StdEncoding.EncodeToString(lsID), conn)
}

func (t *transactionCache) remove(lsID []byte) {
	t.c.Remove(b64.StdEncoding.EncodeToString(lsID))
}
