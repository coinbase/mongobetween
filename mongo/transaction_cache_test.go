package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
)

type txnMockConnection struct {
	i int
}

func (*txnMockConnection) WriteWireMessage(context.Context, []byte) error  { return nil }
func (*txnMockConnection) ReadWireMessage(context.Context) ([]byte, error) { return nil, nil }
func (*txnMockConnection) Description() description.Server                 { return description.Server{} }
func (*txnMockConnection) Close() error                                    { return nil }
func (*txnMockConnection) ID() string                                      { return "" }
func (*txnMockConnection) ServerConnectionID() *int64                      { return nil }
func (*txnMockConnection) DriverConnectionID() uint64                      { return 0 }
func (*txnMockConnection) Address() address.Address                        { return address.Address("") }
func (*txnMockConnection) Stale() bool                                     { return false }

func TestTransactionCacheCount(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x1}, &txnMockConnection{})
	tc.add([]byte{0x2}, &txnMockConnection{})
	tc.add([]byte{0x3}, &txnMockConnection{})
	assert.Equal(t, 3, tc.count())

	tc.add([]byte{0x1}, &txnMockConnection{10})
	assert.Equal(t, 3, tc.count())

	tc.add([]byte{0x10}, &txnMockConnection{10})
	assert.Equal(t, 4, tc.count())

}

func TestTransactionCachePeek(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x10}, &txnMockConnection{4})
	tc.add([]byte{0x12}, &txnMockConnection{5})
	tc.add([]byte{0x14}, &txnMockConnection{6})

	s, ok := tc.peek([]byte{0x12})
	assert.True(t, ok)
	assert.Equal(t, s.(*txnMockConnection).i, 5)

	_, ok = tc.peek([]byte{0x13})
	assert.False(t, ok)
}

func TestTransactionCacheAdd(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x10}, &txnMockConnection{4})
	tc.add([]byte{0x12}, &txnMockConnection{5})
	tc.add([]byte{0x14}, &txnMockConnection{6})

	_, ok := tc.peek([]byte{0x13})
	assert.False(t, ok)

	tc.add([]byte{0x13}, &txnMockConnection{7})
	_, ok = tc.peek([]byte{0x13})
	assert.True(t, ok)
}

func TestTransactionTestRemove(t *testing.T) {
	tc := newTransactionCache()
	tc.add([]byte{0x10}, &txnMockConnection{4})
	tc.add([]byte{0x12}, &txnMockConnection{5})
	tc.add([]byte{0x14}, &txnMockConnection{6})
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
