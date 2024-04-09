package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

var _ driver.Connection = &mockConnection{}

type mockConnection struct {
	i int
}

func (*mockConnection) WriteWireMessage(context.Context, []byte) error  { return nil }
func (*mockConnection) ReadWireMessage(context.Context) ([]byte, error) { return nil, nil }
func (*mockConnection) Description() description.Server                 { return description.Server{} }
func (*mockConnection) Close() error                                    { return nil }
func (*mockConnection) ID() string                                      { return "" }
func (*mockConnection) ServerConnectionID() *int64                      { return nil }
func (*mockConnection) DriverConnectionID() uint64                      { return 0 }
func (*mockConnection) Address() address.Address                        { return address.Address("") }
func (*mockConnection) Stale() bool                                     { return false }

func (m *mockConnection) MinRTT() time.Duration {
	return time.Duration(0)
}

func (m *mockConnection) RTT90() time.Duration {
	return time.Duration(0)
}

func (m *mockConnection) RTTMonitor() driver.RTTMonitor {
	return nil
}

func TestCount(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockConnection{})
	cc.add(12, "db.coll", &mockConnection{})
	cc.add(14, "db.coll", &mockConnection{})
	assert.Equal(t, 3, cc.count())

	cc.add(10, "db.coll", &mockConnection{10})
	assert.Equal(t, 3, cc.count())

	cc.add(10, "db.coll2", &mockConnection{10})
	assert.Equal(t, 4, cc.count())

}

func TestPeek(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockConnection{4})
	cc.add(12, "db.coll", &mockConnection{5})
	cc.add(14, "db.coll", &mockConnection{6})

	s, ok := cc.peek(12, "db.coll")
	assert.True(t, ok)
	assert.Equal(t, s.(*mockConnection).i, 5)

	_, ok = cc.peek(13, "db.coll")
	assert.False(t, ok)
}

func TestAdd(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockConnection{4})
	cc.add(12, "db.coll", &mockConnection{5})
	cc.add(14, "db.coll", &mockConnection{6})

	_, ok := cc.peek(13, "db.coll")
	assert.False(t, ok)

	cc.add(13, "db.coll", &mockConnection{7})
	_, ok = cc.peek(13, "db.coll")
	assert.True(t, ok)
}

func TestRemove(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockConnection{4})
	cc.add(12, "db.coll", &mockConnection{5})
	cc.add(14, "db.coll", &mockConnection{6})
	assert.Equal(t, 3, cc.count())

	_, ok := cc.peek(12, "db.coll")
	assert.True(t, ok)

	cc.remove(12, "db.coll")
	_, ok = cc.peek(12, "db.coll")
	assert.False(t, ok)
	assert.Equal(t, 2, cc.count())

	cc.remove(13, "db.coll")
	assert.Equal(t, 2, cc.count())
}
