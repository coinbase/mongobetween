package mongo

import (
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"testing"
)

func TestOpQuery(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "name", Value: "Misty"}})
	assert.Nil(t, err)

	doc2, err := bson.Marshal(bson.D{{Key: "name", Value: "Brock"}})
	assert.Nil(t, err)

	op := opQuery{
		flags:                wiremessage.AwaitData,
		collName:             "trainers",
		numberToSkip:         5,
		numberToReturn:       7,
		query:                doc1,
		returnFieldsSelector: doc2,
	}
	wm := op.Encode(0)

	dec, err := Decode(wm)
	assert.Nil(t, err)

	msg := dec.(*opQuery)
	assert.Equal(t, wiremessage.AwaitData, msg.flags)
	assert.Equal(t, "trainers", msg.collName)
	assert.Equal(t, int32(5), msg.numberToSkip)
	assert.Equal(t, int32(7), msg.numberToReturn)
	assert.Equal(t, doc1, []byte(msg.query))
	assert.Equal(t, doc2, []byte(msg.returnFieldsSelector))
}

func TestOpQueryCursorID(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "getMore", Value: int64(101)}})
	assert.Nil(t, err)

	op := opQuery{query: doc}
	cursorID, ok := op.CursorID()
	assert.True(t, ok)
	assert.Equal(t, int64(101), cursorID)
}

func TestOpQueryIsIsMaster(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "ismaster", Value: 1}})
	assert.Nil(t, err)

	op := opQuery{
		collName: "admin.$cmd",
		query:    doc,
	}
	assert.True(t, op.IsIsMaster())
}

func TestOpMsg(t *testing.T) {
	insert, err := bson.Marshal(bson.D{{Key: "insert", Value: "trainers"}})
	assert.Nil(t, err)

	doc1, err := bson.Marshal(bson.D{{Key: "name", Value: "Misty"}})
	assert.Nil(t, err)

	doc2, err := bson.Marshal(bson.D{{Key: "name", Value: "Brock"}})
	assert.Nil(t, err)

	op := opMsg{
		flags: wiremessage.MoreToCome,
		sections: []opMsgSection{
			&opMsgSectionSingle{insert},
			&opMsgSectionSequence{
				identifier: "documents",
				msgs:       []bsoncore.Document{doc1, doc2},
			},
		},
	}
	wm := op.Encode(0)

	dec, err := Decode(wm)
	assert.Nil(t, err)

	msg := dec.(*opMsg)
	assert.Equal(t, wiremessage.MoreToCome, msg.flags)
	single := msg.sections[0].(*opMsgSectionSingle)
	sequence := msg.sections[1].(*opMsgSectionSequence)
	assert.Equal(t, insert, []byte(single.msg))
	assert.Equal(t, "documents", sequence.identifier)
	assert.Equal(t, doc1, []byte(sequence.msgs[0]))
	assert.Equal(t, doc2, []byte(sequence.msgs[1]))
}

func TestOpMsgIsIsMaster(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "ismaster", Value: 1},
		{Key: "$db", Value: "admin"},
	})
	assert.Nil(t, err)

	op := opMsg{
		sections: []opMsgSection{&opMsgSectionSingle{msg: doc}},
	}
	assert.True(t, op.IsIsMaster())
}

func TestOpMsgCursorID(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "getMore", Value: int64(102)}})
	assert.Nil(t, err)

	op := opMsg{sections: []opMsgSection{&opMsgSectionSingle{doc1}}}

	cursorID, ok := op.CursorID()
	assert.True(t, ok)
	assert.Equal(t, int64(102), cursorID)

	doc2, err := bson.Marshal(bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(103)}}}})
	assert.Nil(t, err)

	op = opMsg{sections: []opMsgSection{&opMsgSectionSingle{doc2}}}

	cursorID, ok = op.CursorID()
	assert.True(t, ok)
	assert.Equal(t, int64(103), cursorID)
}

func TestOpMsgNoError(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "getMore", Value: int64(102)}, {Key: "ok", Value: int32(1)}})
	assert.Nil(t, err)

	op := opMsg{sections: []opMsgSection{&opMsgSectionSingle{doc1}}}
	err = op.Error()
	assert.Nil(t, err)
}

func TestOpMsgError(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "code", Value: int32(11600)}})
	assert.Nil(t, err)

	op := opMsg{sections: []opMsgSection{&opMsgSectionSingle{doc1}}}
	err = op.Error()
	assert.Error(t, err)
}

func TestOpReply(t *testing.T) {
	doc1, err := bson.Marshal(bson.D{{Key: "name", Value: "Misty"}})
	assert.Nil(t, err)

	doc2, err := bson.Marshal(bson.D{{Key: "name", Value: "Brock"}})
	assert.Nil(t, err)

	op := opReply{
		flags:        wiremessage.ShardConfigStale,
		cursorID:     7,
		startingFrom: 8,
		numReturned:  9,
		documents:    []bsoncore.Document{doc1, doc2},
	}
	wm := op.Encode(0)

	dec, err := Decode(wm)
	assert.Nil(t, err)

	msg := dec.(*opReply)
	assert.Equal(t, wiremessage.ShardConfigStale, msg.flags)
	assert.Equal(t, int64(7), msg.cursorID)
	assert.Equal(t, int32(8), msg.startingFrom)
	assert.Equal(t, int32(9), msg.numReturned)
	assert.Equal(t, doc1, []byte(msg.documents[0]))
	assert.Equal(t, doc2, []byte(msg.documents[1]))
}

func TestOpGetMore(t *testing.T) {
	op := opGetMore{
		fullCollectionName: "trainers",
		numberToReturn:     6,
		cursorID:           7,
	}
	wm := op.Encode(0)

	dec, err := Decode(wm)
	assert.Nil(t, err)

	msg := dec.(*opGetMore)
	assert.Equal(t, "trainers", msg.fullCollectionName)
	assert.Equal(t, int32(6), msg.numberToReturn)
	assert.Equal(t, int64(7), msg.cursorID)
}
