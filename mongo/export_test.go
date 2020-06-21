package mongo

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"testing"
)

func NewOpMsg(single bsoncore.Document, sequence []bsoncore.Document) *Message {
	sections := []opMsgSection{&opMsgSectionSingle{single}}
	if len(sequence) > 0 {
		sections = append(sections, &opMsgSectionSequence{
			identifier: "documents",
			msgs:       sequence,
		})
	}
	op := opMsg{sections: sections}
	wm := op.Encode(0)
	return &Message{wm, &op}
}

func ExtractSingleOpMsg(t *testing.T, msg *Message) bsoncore.Document {
	op, ok := msg.Op.(*opMsg)
	assert.True(t, ok)
	assert.Equal(t, 1, len(op.sections))
	single, ok := op.sections[0].(*opMsgSectionSingle)
	assert.True(t, ok)
	return single.msg
}

func SelectServer(t *testing.T, m *Mongo) *topology.SelectedServer {
	s, err := m.selectServer(context.Background(), 0)
	assert.Nil(t, err)
	return s
}
