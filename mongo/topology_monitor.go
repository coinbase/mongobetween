package mongo

import (
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"
)

func topologyMonitor(log *zap.Logger, t *topology.Topology) {
	last := t.Description()
	for {
		time.Sleep(5 * time.Second)
		desc := t.Description()
		if !topologyDescriptionEqual(&last, &desc) {
			log.Info("Topology changed", topologyChangedFields(&last, &desc)...)
		}
		last = desc
	}
}

func topologyChangedFields(last, desc *description.Topology) (fields []zap.Field) {
	fields = append(fields, zap.String("old_kind", last.Kind.String()))
	fields = append(fields, zap.String("new_kind", desc.Kind.String()))

	lastServers := sortServers(last.Servers)
	descServers := sortServers(desc.Servers)

	for i, s := range lastServers {
		fields = append(fields, zap.String(fmt.Sprintf("old_address_%d", i), s.Addr.String()))
		fields = append(fields, zap.String(fmt.Sprintf("old_kind_%d", i), s.Kind.String()))
	}
	for i, s := range descServers {
		fields = append(fields, zap.String(fmt.Sprintf("new_address_%d", i), s.Addr.String()))
		fields = append(fields, zap.String(fmt.Sprintf("new_kind_%d", i), s.Kind.String()))
	}

	return
}

func sortServers(s []description.Server) []description.Server {
	r := make([]description.Server, len(s))
	copy(r, s)
	sort.Slice(r, func(i, j int) bool {
		return r[i].Addr < r[j].Addr
	})
	return r
}

func topologyDescriptionEqual(d1, d2 *description.Topology) bool {
	if d1.Kind != d2.Kind {
		return false
	}
	s1 := make(map[string]description.ServerKind)
	s2 := make(map[string]description.ServerKind)
	for _, s := range d1.Servers {
		s1[s.Addr.String()] = s.Kind
	}
	for _, s := range d2.Servers {
		s2[s.Addr.String()] = s.Kind
	}
	if len(s1) != len(s2) {
		return false
	}
	for k, v := range s1 {
		if s2[k] != v {
			return false
		}
	}
	return true
}
