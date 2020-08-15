package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// hard-coded response, emulating an InterruptedDueToStepDown error which doesn't drain the downstream connection pool
func InterruptedDueToShutdownResponse(responseTo int32) (*Message, error) {
	doc, err := interruptedDueToShutdownDocument()
	if err != nil {
		return nil, err
	}
	op := opMsg{
		sections: []opMsgSection{&opMsgSectionSingle{
			msg: doc,
		}},
	}
	wm := op.Encode(responseTo)
	return &Message{
		Wm: wm,
		Op: &op,
	}, nil
}

func interruptedDueToShutdownDocument() (bsoncore.Document, error) {
	return bson.Marshal(bson.D{
		{Key: "ok", Value: 0},
		{Key: "code", Value: 11602},
		{Key: "codeName", Value: "InterruptedDueToStepDown"},
		{Key: "errmsg", Value: "interrupted due to shutdown"},
	})
}
