package mongo

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"testing"
	"time"
)

type transactionMockServer struct {
	i int
}

func (m *transactionMockServer) MinRTT() time.Duration {
	return time.Duration(0)
}

func (m *transactionMockServer) RTT90() time.Duration {
	return time.Duration(0)
}

func (m *transactionMockServer) Connection(context.Context) (driver.Connection, error) {
	return nil, nil
}

func TestTransactionCacheCount(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x1}, &transactionMockServer{})
	tc.add([]byte{0x2}, &transactionMockServer{})
	tc.add([]byte{0x3}, &transactionMockServer{})
	assert.Equal(t, 3, tc.count())

	tc.add([]byte{0x1}, &transactionMockServer{10})
	assert.Equal(t, 3, tc.count())

	tc.add([]byte{0x10}, &transactionMockServer{10})
	assert.Equal(t, 4, tc.count())

}

func TestTransactionCachePeek(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x10}, &transactionMockServer{4})
	tc.add([]byte{0x12}, &transactionMockServer{5})
	tc.add([]byte{0x14}, &transactionMockServer{6})

	s, ok := tc.peek([]byte{0x12})
	assert.True(t, ok)
	assert.Equal(t, s.(*transactionMockServer).i, 5)

	_, ok = tc.peek([]byte{0x13})
	assert.False(t, ok)
}

func TestTransactionCacheAdd(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x10}, &transactionMockServer{4})
	tc.add([]byte{0x12}, &transactionMockServer{5})
	tc.add([]byte{0x14}, &transactionMockServer{6})

	_, ok := tc.peek([]byte{0x13})
	assert.False(t, ok)

	tc.add([]byte{0x13}, &transactionMockServer{7})
	_, ok = tc.peek([]byte{0x13})
	assert.True(t, ok)
}

func TestTransactionTestRemove(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x10}, &transactionMockServer{4})
	tc.add([]byte{0x12}, &transactionMockServer{5})
	tc.add([]byte{0x14}, &transactionMockServer{6})
	assert.Equal(t, 3, tc.count())

	_, ok := tc.peek([]byte{0x12})
	assert.True(t, ok)

	tc.remove([]byte{0x12})
	_, ok = tc.peek([]byte{0x12})
	assert.False(t, ok)
	assert.Equal(t, 2, tc.count())

	tc.remove([]byte{0x13})
	assert.Equal(t, 2, tc.count())
}
