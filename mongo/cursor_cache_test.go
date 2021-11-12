package mongo

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"testing"
	"time"
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

func TestCount(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &mockServer{})
	cc.add(12, &mockServer{})
	cc.add(14, &mockServer{})
	assert.Equal(t, 3, cc.count())

	cc.add(10, &mockServer{10})
	assert.Equal(t, 3, cc.count())
}

func TestPeek(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &mockServer{4})
	cc.add(12, &mockServer{5})
	cc.add(14, &mockServer{6})

	s, ok := cc.peek(12)
	assert.True(t, ok)
	assert.Equal(t, s.(*mockServer).i, 5)

	_, ok = cc.peek(13)
	assert.False(t, ok)
}

func TestAdd(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &mockServer{4})
	cc.add(12, &mockServer{5})
	cc.add(14, &mockServer{6})

	_, ok := cc.peek(13)
	assert.False(t, ok)

	cc.add(13, &mockServer{7})
	_, ok = cc.peek(13)
	assert.True(t, ok)
}

func TestRemove(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &mockServer{4})
	cc.add(12, &mockServer{5})
	cc.add(14, &mockServer{6})
	assert.Equal(t, 3, cc.count())

	_, ok := cc.peek(12)
	assert.True(t, ok)

	cc.remove(12)
	_, ok = cc.peek(12)
	assert.False(t, ok)
	assert.Equal(t, 2, cc.count())

	cc.remove(13)
	assert.Equal(t, 2, cc.count())
}
