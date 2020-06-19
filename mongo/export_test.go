package mongo

import (
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"testing"
)

func NewOpMsg(single bsoncore.Document, sequence []bsoncore.Document) *Message {
	op := opMsg{
		sections: []opMsgSection{
			&opMsgSectionSingle{single},
			&opMsgSectionSequence{
				identifier: "documents",
				msgs:       sequence,
			},
		},
	}
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
