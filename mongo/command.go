package mongo

import (
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type Command string

const (
	Unknown           Command = "unknown"
	AbortTransaction  Command = "abortTransaction"
	Aggregate         Command = "aggregate"
	CommitTransaction Command = "commandTransaction"
	Count             Command = "count"
	CreateIndexes     Command = "createIndexes"
	Delete            Command = "delete"
	Distinct          Command = "distinct"
	Drop              Command = "drop"
	DropDatabase      Command = "dropDatabase"
	DropIndexes       Command = "dropIndexes"
	EndSessions       Command = "endSessions"
	Find              Command = "find"
	FindAndModify     Command = "findAndModify"
	GetMore           Command = "getMore"
	Insert            Command = "insert"
	IsMaster          Command = "isMaster"
	Ismaster          Command = "ismaster"
	ListCollections   Command = "listCollections"
	ListIndexes       Command = "listIndexes"
	ListDatabases     Command = "listDatabases"
	MapReduce         Command = "mapReduce"
	Update            Command = "update"
)

var collectionCommands = []Command{Aggregate, Count, CreateIndexes, Delete, Distinct, Drop, DropIndexes, Find, FindAndModify, Insert, ListIndexes, MapReduce, Update}
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
	for _, s := range collectionCommands {
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
	isMaster, _ := doc.Lookup(string(IsMaster)).Int32OK()
	ismaster, _ := doc.Lookup(string(Ismaster)).Int32OK()
	return ismaster+isMaster > 0
}
