package mongo

import (
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"testing"
)

func TestTopologyDescriptionEqual(t *testing.T) {
	d1 := description.Topology{}
	d2 := description.Topology{}
	assert.True(t, topologyDescriptionEqual(&d1, &d2))

	d1 = description.Topology{Kind: description.ReplicaSet}
	d2 = description.Topology{Kind: description.ReplicaSetNoPrimary}
	assert.False(t, topologyDescriptionEqual(&d1, &d2))

	d1 = description.Topology{Servers: []description.Server{{Addr: "addr1", Kind: description.Standalone}}}
	d2 = description.Topology{Servers: []description.Server{{Addr: "addr2", Kind: description.Standalone}}}
	assert.False(t, topologyDescriptionEqual(&d1, &d2))

	d1 = description.Topology{Servers: []description.Server{{Addr: "addr1", Kind: description.Standalone}}}
	d2 = description.Topology{Servers: []description.Server{{Addr: "addr1", Kind: description.Mongos}}}
	assert.False(t, topologyDescriptionEqual(&d1, &d2))

	d1 = description.Topology{Servers: []description.Server{
		{Addr: "addr1", Kind: description.Standalone},
		{Addr: "addr2", Kind: description.Mongos},
	}}
	d2 = description.Topology{Servers: []description.Server{
		{Addr: "addr2", Kind: description.Mongos},
		{Addr: "addr1", Kind: description.Standalone},
	}}
	assert.True(t, topologyDescriptionEqual(&d1, &d2))
}
