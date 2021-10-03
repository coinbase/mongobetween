package mongo

import (
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type Command string

const (
	Unknown           Command = "unknown"
	AbortTransaction          = "abortTransaction"
	Aggregate                 = "aggregate"
	CommitTransaction         = "commandTransaction"
	Count                     = "count"
	CreateIndexes             = "createIndexes"
	Delete                    = "delete"
	Distinct                  = "distinct"
	Drop                      = "drop"
	DropDatabase              = "dropDatabase"
	DropIndexes               = "dropIndexes"
	EndSessions               = "endSessions"
	Find                      = "find"
	FindAndModify             = "findAndModify"
	GetMore                   = "getMore"
	Insert                    = "insert"
	IsMaster                  = "isMaster"
	Ismaster                  = "ismaster"
	ListCollections           = "listCollections"
	ListIndexes               = "listIndexes"
	ListDatabases             = "listDatabases"
	MapReduce                 = "mapReduce"
	Update                    = "update"
)

var collectionStrings = []Command{Aggregate, Count, CreateIndexes, Delete, Distinct, Drop, DropIndexes, Find, FindAndModify, Insert, ListIndexes, MapReduce, Update}
var int32Commands = []Command{AbortTransaction, Aggregate, CommitTransaction, DropDatabase, IsMaster, Ismaster, ListCollections, ListDatabases}
var int64Commands = []Command{GetMore}
var arrayCommands = []Command{EndSessions}

func IsWrite(command Command) bool {
	switch command {
	case CommitTransaction, CreateIndexes, Delete, Drop, DropIndexes, DropDatabase, FindAndModify, Insert, Update:
		return true
	}
	return false
}

func CommandAndCollection(msg bsoncore.Document) (Command, string) {
	for _, s := range collectionStrings {
		if coll, ok := msg.Lookup(string(s)).StringValueOK(); ok {
			return s, coll
		}
	}
	for _, s := range int32Commands {
		value := msg.Lookup(string(s))
		if value.Data != nil {
			return s, ""
		}
	}
	for _, s := range int64Commands {
		value := msg.Lookup(string(s))
		if value.Data != nil {
			return s, ""
		}
	}
	for _, s := range arrayCommands {
		value := msg.Lookup(string(s))
		if value.Data != nil {
			return s, ""
		}
	}
	return Unknown, ""
}

func IsIsMasterDoc(doc bsoncore.Document) bool {
	isMaster, _ := doc.Lookup(IsMaster).Int32OK()
	ismaster, _ := doc.Lookup(Ismaster).Int32OK()
	return ismaster+isMaster > 0
}
