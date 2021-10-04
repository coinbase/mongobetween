package mongo

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// https://github.com/mongodb/mongo/blob/ca57a4d640aee04ef373a50b24e79d85f0bb91a0/src/mongo/client/constants.h#L50
const resultFlagAwaitCapable = wiremessage.ReplyFlag(8)

// hard-coded response, emulating an upstream isMaster response from MongoDB
func IsMasterResponse(responseTo int32, topologyKind description.TopologyKind) (*Message, error) {
	imd, err := isMasterDocument(topologyKind)
	if err != nil {
		return nil, err
	}
	reply := opReply{
		flags:       resultFlagAwaitCapable,
		numReturned: 1,
		documents:   []bsoncore.Document{imd},
	}
	wm := reply.Encode(responseTo)
	return &Message{
		Wm: wm,
		Op: &reply,
	}, nil
}

func isMasterDocument(kind description.TopologyKind) (bsoncore.Document, error) {
	ns := time.Now().UnixNano()
	ms := ns / 1e6
	var doc bson.D
	if kind == description.Single {
		doc = bson.D{
			{Key: "ismaster", Value: true},
			{Key: "maxBsonObjectSize", Value: 16777216},                  // $numberInt
			{Key: "maxMessageSizeBytes", Value: 48000000},                // $numberInt
			{Key: "maxWriteBatchSize", Value: 100000},                    // $numberInt
			{Key: "localTime", Value: bson.D{{Key: "$date", Value: ms}}}, // $numberLong
			{Key: "logicalSessionTimeoutMinutes", Value: 30},             // $numberInt
			{Key: "maxWireVersion", Value: 8},                            // $numberInt
			{Key: "minWireVersion", Value: 0},                            // $numberInt
			{Key: "readOnly", Value: false},
			{Key: "ok", Value: 1.0}, // $numberDouble
		}
	} else {
		// IMPORTANT: if not Single, assumes Sharded!
		doc = bson.D{
			{Key: "ismaster", Value: true},
			{Key: "msg", Value: "isdbgrid"},
			{Key: "maxBsonObjectSize", Value: 16777216},                  // $numberInt
			{Key: "maxMessageSizeBytes", Value: 48000000},                // $numberInt
			{Key: "maxWriteBatchSize", Value: 100000},                    // $numberInt
			{Key: "localTime", Value: bson.D{{Key: "$date", Value: ms}}}, // $numberLong
			{Key: "logicalSessionTimeoutMinutes", Value: 30},             // $numberInt
			{Key: "maxWireVersion", Value: 8},                            // $numberInt
			{Key: "minWireVersion", Value: 0},                            // $numberInt
			{Key: "saslSupportedMechs", Value: bson.A{}},                 // empty (proxy doesn't support auth)
			{Key: "ok", Value: 1.0},                                      // $numberDouble
		}
	}
	return bson.Marshal(doc)
}
