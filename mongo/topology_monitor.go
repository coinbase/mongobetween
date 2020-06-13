package mongo

import (
	"fmt"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"
	"time"
)

func topologyMonitor(log *zap.Logger, t *topology.Topology) {
	last := t.Description()
	for {
		time.Sleep(5 * time.Second)
		desc := t.Description()
		if !topologyDescriptionEqual(&last, &desc) {
			fields := []zap.Field{
				zap.String("old_kind", last.Kind.String()),
				zap.String("new_kind", desc.Kind.String()),
			}
			for i, s := range last.Servers {
				fields = append(fields, zap.String(fmt.Sprintf("old_address_%d", i), s.Addr.String()))
				fields = append(fields, zap.String(fmt.Sprintf("old_kind_%d", i), s.Kind.String()))
			}
			for i, s := range desc.Servers {
				fields = append(fields, zap.String(fmt.Sprintf("new_address_%d", i), s.Addr.String()))
				fields = append(fields, zap.String(fmt.Sprintf("new_kind_%d", i), s.Kind.String()))
			}
			log.Info("Topology changed", fields...)
		}
		last = desc
	}
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
