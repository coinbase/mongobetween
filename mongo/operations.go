package mongo

import (
	"errors"
	"fmt"
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L1361-L1426
func Decode(wm []byte) (Operation, error) {
	wmLength := len(wm)
	length, reqID, _, opCode, wmBody, ok := wiremessage.ReadHeader(wm)
	if !ok || int(length) > wmLength {
		return nil, errors.New("malformed wire message: insufficient bytes")
	}

	var op Operation
	var err error
	switch opCode {
	case wiremessage.OpQuery:
		op, err = decodeQuery(reqID, wmBody)
	case wiremessage.OpMsg:
		op, err = decodeMsg(reqID, wmBody)
	case wiremessage.OpReply:
		op, err = decodeReply(reqID, wmBody)
	case wiremessage.OpGetMore:
		op, err = decodeGetMore(reqID, wmBody)
	default:
		op = &opUnknown{
			opCode: opCode,
			reqID:  reqID,
			wm:     wm,
		}
	}
	if err != nil {
		return nil, err
	}
	return op, nil
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/topology/server_test.go#L968-L1003
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation_legacy.go#L179-L189
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
	checksum uint32
}

type opMsgSection interface {
	cursorID() (cursorID int64, ok bool)
	isIsMaster() bool
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

func (o *opMsgSectionSingle) isIsMaster() bool {
	if db, ok := o.msg.Lookup("$db").StringValueOK(); ok && db == "admin" {
		ismaster, _ := o.msg.Lookup("ismaster").Int32OK()
		isMaster, _ := o.msg.Lookup("isMaster").Int32OK()
		return ismaster+isMaster > 0
	}
	return false
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

func (o *opMsgSectionSequence) isIsMaster() bool {
	return false
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L1387-L1423
func decodeMsg(reqID int32, wm []byte) (*opMsg, error) {
	var ok bool
	m := opMsg{
		reqID: reqID,
	}

	m.flags, wm, ok = wiremessage.ReadMsgFlags(wm)
	if !ok {
		return nil, errors.New("malformed wire message: missing OP_MSG flags")
	}

	checksumPresent := m.flags&wiremessage.ChecksumPresent == wiremessage.ChecksumPresent
	for len(wm) > 0 {
		// If the checksumPresent flag is set, the last four bytes of the message contain the checksum.
		if checksumPresent && len(wm) == 4 {
			m.checksum, wm, ok = wiremessage.ReadMsgChecksum(wm)
			if !ok {
				return nil, errors.New("malformed wire message: insufficient bytes to read checksum")
			}
			continue
		}

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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L898-L904
func (m *opMsg) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpMsg)
	buffer = wiremessage.AppendMsgFlags(buffer, m.flags)
	for _, section := range m.sections {
		buffer = section.append(buffer)
	}
	if m.flags&wiremessage.ChecksumPresent == wiremessage.ChecksumPresent {
		// The checksum is a uint32, but we can use appendi32 to encode it. Overflow/underflow when casting to int32 is
		// not a concern here because the bytes in the number do not change after casting.
		buffer = appendi32(buffer, int32(m.checksum))
	}
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (m *opMsg) IsIsMaster() bool {
	for _, section := range m.sections {
		if section.isIsMaster() {
			return true
		}
	}
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
	return driver.ExtractErrorFromServerResponse(single.msg)
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L1297-L1358
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/drivertest/channel_conn.go#L73-L82
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
	return driver.ExtractErrorFromServerResponse(r.documents[0])
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L1297-L1358
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation_legacy.go#L284-L291
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
