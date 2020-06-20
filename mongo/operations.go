package mongo

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/mongo/driver"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type Message struct {
	Wm []byte
	Op Operation
}

type Operation interface {
	OpCode() wiremessage.OpCode
	Encode(responseTo int32) []byte
	IsIsMaster() bool
	CursorID() (cursorID int64, ok bool)
	RequestID() int32
	Error() error
	Unacknowledged() bool
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L1165-L1230
func Decode(wm []byte) (Operation, error) {
	wmLength := len(wm)
	length, reqID, _, opCode, wmBody, ok := wiremessage.ReadHeader(wm)
	if !ok || int(length) > wmLength {
		return nil, errors.New("malformed wire message: insufficient bytes")
	}

	switch opCode {
	case wiremessage.OpQuery:
		query, err := decodeQuery(reqID, wmBody)
		if err != nil {
			return nil, err
		}
		return query, nil
	case wiremessage.OpMsg:
		msg, err := decodeMsg(reqID, wmBody)
		if err != nil {
			return nil, err
		}
		return msg, nil
	case wiremessage.OpReply:
		msg, err := decodeReply(reqID, wmBody)
		if err != nil {
			return nil, err
		}
		return msg, nil
	case wiremessage.OpGetMore:
		msg, err := decodeGetMore(reqID, wmBody)
		if err != nil {
			return nil, err
		}
		return msg, nil
	default:
		return &opUnknown{
			opCode: opCode,
			reqID:  reqID,
			wm:     wm,
		}, nil
	}
}

type opUnknown struct {
	opCode wiremessage.OpCode
	reqID  int32
	wm     []byte
}

func (o *opUnknown) OpCode() wiremessage.OpCode {
	return o.opCode
}

func (o *opUnknown) Encode(responseTo int32) []byte {
	return o.wm
}

func (o *opUnknown) IsIsMaster() bool {
	return false
}

func (o *opUnknown) CursorID() (cursorID int64, ok bool) {
	return 0, false
}

func (o *opUnknown) RequestID() int32 {
	return o.reqID
}

func (o *opUnknown) Error() error {
	return nil
}

func (o *opUnknown) Unacknowledged() bool {
	return false
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-query
type opQuery struct {
	reqID                int32
	flags                wiremessage.QueryFlag
	collName             string
	numberToSkip         int32
	numberToReturn       int32
	query                bsoncore.Document
	returnFieldsSelector bsoncore.Document
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/topology/server_test.go#L302-L337
func decodeQuery(reqID int32, wm []byte) (*opQuery, error) {
	var ok bool
	q := opQuery{
		reqID: reqID,
	}

	q.flags, wm, ok = wiremessage.ReadQueryFlags(wm)
	if !ok {
		return nil, errors.New("malformed query message: missing OP_QUERY flags")
	}

	q.collName, wm, ok = wiremessage.ReadQueryFullCollectionName(wm)
	if !ok {
		return nil, errors.New("malformed query message: full collection name")
	}

	q.numberToSkip, wm, ok = wiremessage.ReadQueryNumberToSkip(wm)
	if !ok {
		return nil, errors.New("malformed query message: number to skip")
	}

	q.numberToReturn, wm, ok = wiremessage.ReadQueryNumberToReturn(wm)
	if !ok {
		return nil, errors.New("malformed query message: number to return")
	}

	q.query, wm, ok = wiremessage.ReadQueryQuery(wm)
	if !ok {
		return nil, errors.New("malformed query message: query document")
	}

	if len(wm) > 0 {
		q.returnFieldsSelector, _, ok = wiremessage.ReadQueryReturnFieldsSelector(wm)
		if !ok {
			return nil, errors.New("malformed query message: return fields selector")
		}
	}

	return &q, nil
}

func (q *opQuery) OpCode() wiremessage.OpCode {
	return wiremessage.OpQuery
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation_legacy.go#L172-L184
func (q *opQuery) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpQuery)
	buffer = wiremessage.AppendQueryFlags(buffer, q.flags)
	buffer = wiremessage.AppendQueryFullCollectionName(buffer, q.collName)
	buffer = wiremessage.AppendQueryNumberToSkip(buffer, q.numberToSkip)
	buffer = wiremessage.AppendQueryNumberToReturn(buffer, q.numberToReturn)
	buffer = append(buffer, q.query...)
	if len(q.returnFieldsSelector) != 0 {
		// returnFieldsSelector is optional
		buffer = append(buffer, q.returnFieldsSelector...)
	}
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (q *opQuery) CursorID() (cursorID int64, ok bool) {
	return q.query.Lookup("getMore").Int64OK()
}

func (q *opQuery) RequestID() int32 {
	return q.reqID
}

func (q *opQuery) IsIsMaster() bool {
	if q.collName != "admin.$cmd" {
		return false
	}
	ismaster, _ := q.query.Lookup("ismaster").Int32OK()
	isMaster, _ := q.query.Lookup("isMaster").Int32OK()
	return ismaster+isMaster > 0
}

func (q *opQuery) Error() error {
	return nil
}

func (q *opQuery) Unacknowledged() bool {
	return false
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-msg
type opMsg struct {
	reqID    int32
	flags    wiremessage.MsgFlag
	sections []opMsgSection
}

type opMsgSection interface {
	cursorID() (cursorID int64, ok bool)
	append(buffer []byte) []byte
}

type opMsgSectionSingle struct {
	msg bsoncore.Document
}

func (o *opMsgSectionSingle) cursorID() (cursorID int64, ok bool) {
	if getMore, ok := o.msg.Lookup("getMore").Int64OK(); ok {
		return getMore, ok
	}
	return o.msg.Lookup("cursor", "id").Int64OK()
}

func (o *opMsgSectionSingle) append(buffer []byte) []byte {
	buffer = wiremessage.AppendMsgSectionType(buffer, wiremessage.SingleDocument)
	return append(buffer, o.msg...)
}

type opMsgSectionSequence struct {
	identifier string
	msgs       []bsoncore.Document
}

func (o *opMsgSectionSequence) cursorID() (cursorID int64, ok bool) {
	// assume no cursor IDs are returned in OP_MSG document sequences
	return 0, false
}

func (o *opMsgSectionSequence) append(buffer []byte) []byte {
	buffer = wiremessage.AppendMsgSectionType(buffer, wiremessage.DocumentSequence)

	length := int32(len(o.identifier) + 5)
	for _, msg := range o.msgs {
		length += int32(len(msg))
	}

	buffer = appendi32(buffer, length)
	buffer = appendCString(buffer, o.identifier)
	for _, msg := range o.msgs {
		buffer = append(buffer, msg...)
	}

	return buffer
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L1191-L1220
func decodeMsg(reqID int32, wm []byte) (*opMsg, error) {
	var ok bool
	m := opMsg{
		reqID: reqID,
	}

	m.flags, wm, ok = wiremessage.ReadMsgFlags(wm)
	if !ok {
		return nil, errors.New("malformed wire message: missing OP_MSG flags")
	}

	for len(wm) > 0 {
		var stype wiremessage.SectionType
		stype, wm, ok = wiremessage.ReadMsgSectionType(wm)
		if !ok {
			return nil, errors.New("malformed wire message: insufficient bytes to read section type")
		}

		switch stype {
		case wiremessage.SingleDocument:
			s := opMsgSectionSingle{}
			s.msg, wm, ok = wiremessage.ReadMsgSectionSingleDocument(wm)
			if !ok {
				return nil, errors.New("malformed wire message: insufficient bytes to read single document")
			}
			m.sections = append(m.sections, &s)
		case wiremessage.DocumentSequence:
			s := opMsgSectionSequence{}
			s.identifier, s.msgs, wm, ok = wiremessage.ReadMsgSectionDocumentSequence(wm)
			if !ok {
				return nil, errors.New("malformed wire message: insufficient bytes to read document sequence")
			}
			m.sections = append(m.sections, &s)
		default:
			return nil, fmt.Errorf("malformed wire message: unknown section type %v", stype)
		}
	}

	return &m, nil
}

func (m *opMsg) OpCode() wiremessage.OpCode {
	return wiremessage.OpMsg
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L740-L746
func (m *opMsg) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpMsg)
	buffer = wiremessage.AppendMsgFlags(buffer, m.flags)
	for _, section := range m.sections {
		buffer = section.append(buffer)
	}
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (m *opMsg) IsIsMaster() bool {
	return false
}

func (m *opMsg) CursorID() (cursorID int64, ok bool) {
	for _, section := range m.sections {
		if cursorID, ok := section.cursorID(); ok {
			return cursorID, ok
		}
	}
	return 0, false
}

func (m *opMsg) RequestID() int32 {
	return m.reqID
}

func (m *opMsg) Error() error {
	if len(m.sections) == 0 {
		return nil
	}
	single, ok := m.sections[0].(*opMsgSectionSingle)
	if !ok {
		return nil
	}
	return extractError(single.msg)
}

func (m *opMsg) Unacknowledged() bool {
	return m.flags&wiremessage.MoreToCome == wiremessage.MoreToCome
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-reply
type opReply struct {
	reqID        int32
	flags        wiremessage.ReplyFlag
	cursorID     int64
	startingFrom int32
	numReturned  int32
	documents    []bsoncore.Document
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L1101-L1162
func decodeReply(reqID int32, wm []byte) (*opReply, error) {
	var ok bool
	r := opReply{
		reqID: reqID,
	}

	r.flags, wm, ok = wiremessage.ReadReplyFlags(wm)
	if !ok {
		return nil, errors.New("malformed reply message: missing OP_REPLY flags")
	}

	r.cursorID, wm, ok = wiremessage.ReadReplyCursorID(wm)
	if !ok {
		return nil, errors.New("malformed reply message: cursor id")
	}

	r.startingFrom, wm, ok = wiremessage.ReadReplyStartingFrom(wm)
	if !ok {
		return nil, errors.New("malformed reply message: starting from")
	}

	r.numReturned, wm, ok = wiremessage.ReadReplyNumberReturned(wm)
	if !ok {
		return nil, errors.New("malformed reply message: number returned")
	}

	r.documents, _, ok = wiremessage.ReadReplyDocuments(wm)
	if !ok {
		return nil, errors.New("malformed reply message: could not read documents from reply")
	}

	return &r, nil
}

func (r *opReply) OpCode() wiremessage.OpCode {
	return wiremessage.OpReply
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/drivertest/channel_conn.go#L68-L77
func (r *opReply) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpReply)
	buffer = wiremessage.AppendReplyFlags(buffer, r.flags)
	buffer = wiremessage.AppendReplyCursorID(buffer, r.cursorID)
	buffer = wiremessage.AppendReplyStartingFrom(buffer, r.startingFrom)
	buffer = wiremessage.AppendReplyNumberReturned(buffer, r.numReturned)
	for _, doc := range r.documents {
		buffer = append(buffer, doc...)
	}
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (r *opReply) IsIsMaster() bool {
	return false
}

func (r *opReply) CursorID() (cursorID int64, ok bool) {
	return r.cursorID, true
}

func (r *opReply) RequestID() int32 {
	return r.reqID
}

func (r *opReply) Error() error {
	if len(r.documents) == 0 {
		return nil
	}
	return extractError(r.documents[0])
}

func (r *opReply) Unacknowledged() bool {
	return false
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-get-more
type opGetMore struct {
	reqID              int32
	fullCollectionName string
	numberToReturn     int32
	cursorID           int64
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L1101-L1162
func decodeGetMore(reqID int32, wm []byte) (*opGetMore, error) {
	var ok bool
	g := opGetMore{
		reqID: reqID,
	}

	// the driver doesn't support any ReadGetMore* methods, so reuse methods from other operations

	_, wm, ok = wiremessage.ReadKillCursorsZero(wm)
	if !ok {
		return nil, errors.New("malformed get_more message: missing zero")
	}

	g.fullCollectionName, wm, ok = wiremessage.ReadQueryFullCollectionName(wm)
	if !ok {
		return nil, errors.New("malformed get_more message: missing full collection name")
	}

	g.numberToReturn, wm, ok = wiremessage.ReadQueryNumberToReturn(wm)
	if !ok {
		return nil, errors.New("malformed get_more message: missing number to return")
	}

	g.cursorID, _, ok = wiremessage.ReadReplyCursorID(wm)
	if !ok {
		return nil, errors.New("malformed get_more message: missing cursorID")
	}

	return &g, nil
}

func (g *opGetMore) OpCode() wiremessage.OpCode {
	return wiremessage.OpGetMore
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation_legacy.go#L270-L277
func (g *opGetMore) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpGetMore)
	buffer = wiremessage.AppendGetMoreZero(buffer)
	buffer = wiremessage.AppendGetMoreFullCollectionName(buffer, g.fullCollectionName)
	buffer = wiremessage.AppendGetMoreNumberToReturn(buffer, g.numberToReturn)
	buffer = wiremessage.AppendGetMoreCursorID(buffer, g.cursorID)
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (g *opGetMore) IsIsMaster() bool {
	return false
}

func (g *opGetMore) CursorID() (cursorID int64, ok bool) {
	return g.cursorID, true
}

func (g *opGetMore) RequestID() int32 {
	return g.reqID
}

func (g *opGetMore) Error() error {
	return nil
}

func (g *opGetMore) Unacknowledged() bool {
	return false
}

func appendi32(dst []byte, i32 int32) []byte {
	return append(dst, byte(i32), byte(i32>>8), byte(i32>>16), byte(i32>>24))
}

func appendCString(b []byte, str string) []byte {
	b = append(b, str...)
	return append(b, 0x00)
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/errors.go#L290-L409
func extractError(rdr bsoncore.Document) error {
	var errmsg, codeName string
	var code int32
	var labels []string
	var ok bool
	var wcError driver.WriteCommandError
	elems, err := rdr.Elements()
	if err != nil {
		return err
	}

	for _, elem := range elems {
		switch elem.Key() {
		case "ok":
			switch elem.Value().Type {
			case bson.TypeInt32:
				if elem.Value().Int32() == 1 {
					ok = true
				}
			case bson.TypeInt64:
				if elem.Value().Int64() == 1 {
					ok = true
				}
			case bson.TypeDouble:
				if elem.Value().Double() == 1 {
					ok = true
				}
			}
		case "errmsg":
			if str, okay := elem.Value().StringValueOK(); okay {
				errmsg = str
			}
		case "codeName":
			if str, okay := elem.Value().StringValueOK(); okay {
				codeName = str
			}
		case "code":
			if c, okay := elem.Value().Int32OK(); okay {
				code = c
			}
		case "errorLabels":
			if arr, okay := elem.Value().ArrayOK(); okay {
				elems, err := arr.Elements()
				if err != nil {
					continue
				}
				for _, elem := range elems {
					if str, ok := elem.Value().StringValueOK(); ok {
						labels = append(labels, str)
					}
				}

			}
		case "writeErrors":
			arr, exists := elem.Value().ArrayOK()
			if !exists {
				break
			}
			vals, err := arr.Values()
			if err != nil {
				continue
			}
			for _, val := range vals {
				var we driver.WriteError
				doc, exists := val.DocumentOK()
				if !exists {
					continue
				}
				if index, exists := doc.Lookup("index").AsInt64OK(); exists {
					we.Index = index
				}
				if code, exists := doc.Lookup("code").AsInt64OK(); exists {
					we.Code = code
				}
				if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
					we.Message = msg
				}
				wcError.WriteErrors = append(wcError.WriteErrors, we)
			}
		case "writeConcernError":
			doc, exists := elem.Value().DocumentOK()
			if !exists {
				break
			}
			wcError.WriteConcernError = new(driver.WriteConcernError)
			if code, exists := doc.Lookup("code").AsInt64OK(); exists {
				wcError.WriteConcernError.Code = code
			}
			if name, exists := doc.Lookup("codeName").StringValueOK(); exists {
				wcError.WriteConcernError.Name = name
			}
			if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
				wcError.WriteConcernError.Message = msg
			}
			if info, exists := doc.Lookup("errInfo").DocumentOK(); exists {
				wcError.WriteConcernError.Details = make([]byte, len(info))
				copy(wcError.WriteConcernError.Details, info)
			}
		}
	}

	if !ok {
		if errmsg == "" {
			errmsg = "command failed"
		}

		return driver.Error{
			Code:    code,
			Message: errmsg,
			Name:    codeName,
			Labels:  labels,
		}
	}

	if len(wcError.WriteErrors) > 0 || wcError.WriteConcernError != nil {
		return wcError
	}

	return nil
}
