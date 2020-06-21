package mongo

import (
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"testing"
)

func TestCount(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &topology.SelectedServer{})
	cc.add(12, &topology.SelectedServer{})
	cc.add(14, &topology.SelectedServer{})
	assert.Equal(t, 3, cc.count())

	cc.add(10, &topology.SelectedServer{})
	assert.Equal(t, 3, cc.count())
}

func TestPeek(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &topology.SelectedServer{})
	cc.add(12, &topology.SelectedServer{Kind: 5})
	cc.add(14, &topology.SelectedServer{})

	s, ok := cc.peek(12)
	assert.True(t, ok)
	assert.Equal(t, description.TopologyKind(5), s.Kind)

	_, ok = cc.peek(13)
	assert.False(t, ok)
}

func TestAdd(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &topology.SelectedServer{})
	cc.add(12, &topology.SelectedServer{})
	cc.add(14, &topology.SelectedServer{})

	_, ok := cc.peek(13)
	assert.False(t, ok)

	cc.add(13, &topology.SelectedServer{})
	_, ok = cc.peek(13)
	assert.True(t, ok)
}

func TestRemove(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, &topology.SelectedServer{})
	cc.add(12, &topology.SelectedServer{})
	cc.add(14, &topology.SelectedServer{})
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
