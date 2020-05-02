package mongo

import (
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"testing"
)

func TestIsMasterSingle(t *testing.T) {
	im, err := IsMasterResponse(10, description.Single)
	assert.Nil(t, err)

	op, err := Decode(im.Wm)
	assert.Nil(t, err)

	assert.Equal(t, wiremessage.OpReply, op.OpCode())

	reply := op.(*opReply)
	assert.Equal(t, 1, len(reply.documents))
	doc := reply.documents[0]

	ismaster, ok := doc.Lookup("ismaster").BooleanOK()
	assert.True(t, ok)
	assert.True(t, ismaster)

	_, err = doc.LookupErr("msg")
	assert.Equal(t, bsoncore.ErrElementNotFound, err)
}

func TestIsMasterSharded(t *testing.T) {
	im, err := IsMasterResponse(10, description.Sharded)
	assert.Nil(t, err)

	op, err := Decode(im.Wm)
	assert.Nil(t, err)

	assert.Equal(t, wiremessage.OpReply, op.OpCode())

	reply := op.(*opReply)
	assert.Equal(t, 1, len(reply.documents))
	doc := reply.documents[0]

	ismaster, ok := doc.Lookup("ismaster").BooleanOK()
	assert.True(t, ok)
	assert.True(t, ismaster)

	msg, ok := doc.Lookup("msg").StringValueOK()
	assert.True(t, ok)
	assert.Equal(t, "isdbgrid", msg)
}
