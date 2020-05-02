package mongo

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	uri := "mongodb://localhost:27017/test"
	if os.Getenv("CI") == "true" {
		uri = "mongodb://mongo:27017/test"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	clientOptions := options.Client().ApplyURI(uri)
	m, err := Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	insert, err := bson.Marshal(bson.D{
		{Key: "insert", Value: "trainers"},
		{Key: "$db", Value: "test"},
	})
	assert.Nil(t, err)

	doc1, err := bson.Marshal(bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "name", Value: "Misty"},
		{Key: "age", Value: 10},
		{Key: "city", Value: "Cerulean City"},
	})
	assert.Nil(t, err)

	doc2, err := bson.Marshal(bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "name", Value: "Brock"},
		{Key: "age", Value: 15},
		{Key: "city", Value: "Pewter City"},
	})
	assert.Nil(t, err)

	op := opMsg{
		sections: []opMsgSection{
			&opMsgSectionSingle{insert},
			&opMsgSectionSequence{
				identifier: "documents",
				msgs:       []bsoncore.Document{doc1, doc2},
			},
		},
	}
	wm := op.Encode(0)

	res, err := m.RoundTrip(&Message{wm, &op})
	assert.Nil(t, err)

	msg := res.Op.(*opMsg)
	assert.Equal(t, 1, len(msg.sections))
	single := msg.sections[0].(*opMsgSectionSingle)

	assert.Equal(t, int32(2), single.msg.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.msg.Lookup("ok").Double())
}
