package mongo

import (
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/x/mongo/driver"

	"github.com/coinbase/mongobetween/lruttl"
)

// on a 64-bit machine, 1 million cursors uses around 480mb of memory
const maxCursors = 1024 * 1024

// one day expiry
const cursorExpiry = 24 * time.Hour

type cursorCache struct {
	c *lruttl.Cache
}

func newCursorCache() *cursorCache {
	return &cursorCache{
		c: lruttl.New(maxCursors, cursorExpiry),
	}
}

func (c *cursorCache) count() int {
	return c.c.Len()
}

func (c *cursorCache) peek(cursorID int64, collection string) (server driver.Server, ok bool) {
	v, ok := c.c.Peek(buildKey(cursorID, collection))
	if !ok {
		return
	}
	return v.(driver.Server), true
}

func (c *cursorCache) add(cursorID int64, collection string, server driver.Server) {
	c.c.Add(buildKey(cursorID, collection), server)
}

func (c *cursorCache) remove(cursorID int64, collection string) {
	c.c.Remove(buildKey(cursorID, collection))
}

func (c *cursorCache) peekDualID(cursorID int64, collection string) (int64, bool) {
	v, ok := c.c.Peek(buildKey(cursorID, collection))
	if !ok {
		return 0, ok
	}

	str, ok := v.(string)
	if !ok {
		return 0, ok
	}

	id, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, false
	}
	return int64(id), true
}

func (c *cursorCache) addDualID(cursorID int64, collection string, dualCursorID int64) {
	c.c.Add(buildKey(cursorID, collection), strconv.FormatInt(dualCursorID, 10))
}

func (c *cursorCache) removeDualID(cursorID int64, collection string) {
	c.c.Remove(buildKey(cursorID, collection))
}

func buildKey(cursorID int64, collection string) string {
	return fmt.Sprintf("%d-%s", cursorID, collection)
}
