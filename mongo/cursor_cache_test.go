package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

type mockServer struct {
	i int
}

func (m *mockServer) Connection(context.Context) (driver.Connection, error) {
	return nil, nil
}

func (m *mockServer) MinRTT() time.Duration {
	return time.Duration(0)
}

func (m *mockServer) RTT90() time.Duration {
	return time.Duration(0)
}

func (m *mockServer) RTTMonitor() driver.RTTMonitor {
	return nil
}

func TestCount(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{})
	cc.add(12, "db.coll", &mockServer{})
	cc.add(14, "db.coll", &mockServer{})
	assert.Equal(t, 3, cc.count())

	cc.add(10, "db.coll", &mockServer{10})
	assert.Equal(t, 3, cc.count())

	cc.add(10, "db.coll2", &mockServer{10})
	assert.Equal(t, 4, cc.count())

}

func TestPeek(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{4})
	cc.add(12, "db.coll", &mockServer{5})
	cc.add(14, "db.coll", &mockServer{6})

	s, ok := cc.peek(12, "db.coll")
	assert.True(t, ok)
	assert.Equal(t, s.(*mockServer).i, 5)

	_, ok = cc.peek(13, "db.coll")
	assert.False(t, ok)
}

func TestAdd(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{4})
	cc.add(12, "db.coll", &mockServer{5})
	cc.add(14, "db.coll", &mockServer{6})

	_, ok := cc.peek(13, "db.coll")
	assert.False(t, ok)

	cc.add(13, "db.coll", &mockServer{7})
	_, ok = cc.peek(13, "db.coll")
	assert.True(t, ok)
}

func TestRemove(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{4})
	cc.add(12, "db.coll", &mockServer{5})
	cc.add(14, "db.coll", &mockServer{6})
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
