package mongo

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type Message struct {
	Wm []byte
	Op Operation
}

type TransactionDetails struct {
	LsId               []byte
	TxnNumber          int64
	IsStartTransaction bool
}

type Operation interface {
	fmt.Stringer
	OpCode() wiremessage.OpCode
	Encode(responseTo int32) []byte
	IsIsMaster() bool
	CursorID() (cursorID int64, ok bool)
	RequestID() int32
	Error() error
	Unacknowledged() bool
	CommandAndCollection() (Command, string)
	TransactionDetails() (*TransactionDetails, bool)
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
	case wiremessage.OpUpdate:
		op, err = decodeUpdate(reqID, wmBody)
	case wiremessage.OpInsert:
		op, err = decodeInsert(reqID, wmBody)
	case wiremessage.OpDelete:
		op, err = decodeDelete(reqID, wmBody)
	case wiremessage.OpKillCursors:
		op, err = decodeKillCursors(reqID, wmBody)
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

func (o *opUnknown) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
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

func (o *opUnknown) CommandAndCollection() (Command, string) {
	return Unknown, ""
}

func (o *opUnknown) String() string {
	return fmt.Sprintf("{ OpUnknown opCode: %d, wm: %s }", o.opCode, o.wm)
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-query
type opQuery struct {
	reqID                int32
	flags                wiremessage.QueryFlag
	fullCollectionName   string
	numberToSkip         int32
	numberToReturn       int32
	query                bsoncore.Document
	returnFieldsSelector bsoncore.Document
}

func (q *opQuery) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
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

	q.fullCollectionName, wm, ok = wiremessage.ReadQueryFullCollectionName(wm)
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
	buffer = wiremessage.AppendQueryFullCollectionName(buffer, q.fullCollectionName)
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
	if q.fullCollectionName != "admin.$cmd" {
		return false
	}
	return IsIsMasterDoc(q.query)
}

func (q *opQuery) Error() error {
	return nil
}

func (q *opQuery) Unacknowledged() bool {
	return false
}

func (q *opQuery) CommandAndCollection() (Command, string) {
	return Find, q.fullCollectionName
}

func (q *opQuery) String() string {
	return fmt.Sprintf("{ OpQuery flags: %s, fullCollectionName: %s, numberToSkip: %d, numberToReturn: %d, query: %s, returnFieldsSelector: %s }", q.flags.String(), q.fullCollectionName, q.numberToSkip, q.numberToReturn, q.query.String(), q.returnFieldsSelector.String())
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-msg
type opMsg struct {
	reqID    int32
	flags    wiremessage.MsgFlag
	sections []opMsgSection
	checksum uint32
}

type opMsgSection interface {
	fmt.Stringer
	cursorID() (cursorID int64, ok bool)
	isIsMaster() bool
	append(buffer []byte) []byte
	commandAndCollection() (Command, string)
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
		return IsIsMasterDoc(o.msg)
	}
	return false
}

func (o *opMsgSectionSingle) append(buffer []byte) []byte {
	buffer = wiremessage.AppendMsgSectionType(buffer, wiremessage.SingleDocument)
	return append(buffer, o.msg...)
}

func (o *opMsgSectionSingle) commandAndCollection() (Command, string) {
	return CommandAndCollection(o.msg)
}

func (o *opMsgSectionSingle) String() string {
	return fmt.Sprintf("{ SectionSingle msg: %s }", o.msg.String())
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

func (o *opMsgSectionSequence) commandAndCollection() (Command, string) {
	return Unknown, ""
}

func (o *opMsgSectionSequence) String() string {
	var msgs []string
	for _, msg := range o.msgs {
		msgs = append(msgs, msg.String())
	}
	return fmt.Sprintf("{ SectionSingle identifier: %s, msgs: [%s] }", o.identifier, strings.Join(msgs, ", "))
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

func (m *opMsg) CommandAndCollection() (Command, string) {
	for _, section := range m.sections {
		command, collection := section.commandAndCollection()
		if command != Unknown {
			return command, collection
		}
	}
	return Unknown, ""
}

// TransactionDetails See https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst
// Version 4.0 of the server introduces multi-statement transactions.
// opMsg is available from wire protocol 3.6
// deprecated operations such OP_UPDATE OP_INSERT are not supposed to support transaction statements.
// When constructing any other command within a transaction, drivers MUST add the lsid, txnNumber, and autocommit fields.
func (m *opMsg) TransactionDetails() (*TransactionDetails, bool) {

	for _, section := range m.sections {
		if single, ok := section.(*opMsgSectionSingle); ok {
			var ok bool
			_, lsid, ok := single.msg.Lookup("lsid", "id").BinaryOK()
			if !ok {
				continue
			}
			txnNumber, ok := single.msg.Lookup("txnNumber").Int64OK()
			if !ok {
				continue
			}
			_, ok = single.msg.Lookup("autocommit").BooleanOK()
			if !ok {
				continue
			}

			startTransaction, ok := single.msg.Lookup("startTransaction").BooleanOK()

			return &TransactionDetails{
				LsId:               lsid,
				TxnNumber:          txnNumber,
				IsStartTransaction: ok && startTransaction,
			}, true
		}
	}

	return nil, false
}

func (m *opMsg) String() string {
	var sections []string
	for _, section := range m.sections {
		sections = append(sections, section.String())
	}
	return fmt.Sprintf("{ OpMsg flags: %d, sections: [%s], checksum: %d }", m.flags, strings.Join(sections, ", "), m.checksum)
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

func (r *opReply) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
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

func (r *opReply) CommandAndCollection() (Command, string) {
	return Find, ""
}

func (r *opReply) String() string {
	var documents []string
	for _, document := range r.documents {
		documents = append(documents, document.String())
	}
	return fmt.Sprintf("{ OpReply flags: %d, cursorID: %d, startingFrom: %d, numReturned: %d, documents: [%s] }", r.flags, r.cursorID, r.startingFrom, r.numReturned, strings.Join(documents, ", "))
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-get-more
type opGetMore struct {
	reqID              int32
	fullCollectionName string
	numberToReturn     int32
	cursorID           int64
}

func (g *opGetMore) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
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

func (g *opGetMore) CommandAndCollection() (Command, string) {
	return GetMore, g.fullCollectionName
}

func (g *opGetMore) String() string {
	return fmt.Sprintf("{ OpGetMore fullCollectionName: %s, numberToReturn: %d, cursorID: %d }", g.fullCollectionName, g.numberToReturn, g.cursorID)
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op_update
type opUpdate struct {
	reqID              int32
	fullCollectionName string
	flags              int32
	selector           bsoncore.Document
	update             bsoncore.Document
}

func (u *opUpdate) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
}

func decodeUpdate(reqID int32, wm []byte) (*opUpdate, error) {
	var ok bool
	u := opUpdate{
		reqID: reqID,
	}

	u.fullCollectionName, wm, ok = readCString(wm)
	if !ok {
		return nil, errors.New("malformed update message: full collection name")
	}

	u.flags, wm, ok = readi32(wm)
	if !ok {
		return nil, errors.New("malformed update message: missing OP_UPDATE flags")
	}

	u.selector, wm, ok = bsoncore.ReadDocument(wm)
	if !ok {
		return nil, errors.New("malformed update message: selector document")
	}

	u.update, _, ok = bsoncore.ReadDocument(wm)
	if !ok {
		return nil, errors.New("malformed update message: update document")
	}

	return &u, nil
}

func (u *opUpdate) OpCode() wiremessage.OpCode {
	return wiremessage.OpUpdate
}

func (u *opUpdate) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpUpdate)
	buffer = appendCString(buffer, u.fullCollectionName)
	buffer = appendi32(buffer, u.flags)
	buffer = append(buffer, u.selector...)
	buffer = append(buffer, u.update...)
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (u *opUpdate) IsIsMaster() bool {
	return false
}

func (u *opUpdate) CursorID() (cursorID int64, ok bool) {
	return 0, false
}

func (u *opUpdate) RequestID() int32 {
	return u.reqID
}

func (u *opUpdate) Error() error {
	return nil
}

func (u *opUpdate) Unacknowledged() bool {
	return false
}

func (u *opUpdate) CommandAndCollection() (Command, string) {
	return Update, u.fullCollectionName
}

func (u *opUpdate) String() string {
	return fmt.Sprintf("{ OpQuery fullCollectionName: %s, flags: %d, selector: %s, update: %s }", u.fullCollectionName, u.flags, u.selector.String(), u.update.String())
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op_insert
type opInsert struct {
	reqID              int32
	flags              int32
	fullCollectionName string
	documents          []bsoncore.Document
}

func (i *opInsert) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
}

func decodeInsert(reqID int32, wm []byte) (*opInsert, error) {
	var ok bool
	i := opInsert{
		reqID: reqID,
	}

	i.flags, wm, ok = readi32(wm)
	if !ok {
		return nil, errors.New("malformed insert message: missing OP_INSERT flags")
	}

	i.fullCollectionName, wm, ok = readCString(wm)
	if !ok {
		return nil, errors.New("malformed insert message: full collection name")
	}

	i.documents, _, ok = wiremessage.ReadReplyDocuments(wm)
	if !ok {
		return nil, errors.New("malformed insert message: could not read documents")
	}

	return &i, nil
}

func (i *opInsert) OpCode() wiremessage.OpCode {
	return wiremessage.OpInsert
}

func (i *opInsert) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpInsert)
	buffer = appendi32(buffer, i.flags)
	buffer = appendCString(buffer, i.fullCollectionName)
	for _, doc := range i.documents {
		buffer = append(buffer, doc...)
	}
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (i *opInsert) IsIsMaster() bool {
	return false
}

func (i *opInsert) CursorID() (cursorID int64, ok bool) {
	return 0, false
}

func (i *opInsert) RequestID() int32 {
	return i.reqID
}

func (i *opInsert) Error() error {
	return nil
}

func (i *opInsert) Unacknowledged() bool {
	return false
}

func (i *opInsert) CommandAndCollection() (Command, string) {
	return Insert, i.fullCollectionName
}

func (i *opInsert) String() string {
	var documents []string
	for _, document := range i.documents {
		documents = append(documents, document.String())
	}
	return fmt.Sprintf("{ OpInsert flags: %d, fullCollectionName: %s, documents: %s }", i.flags, i.fullCollectionName, strings.Join(documents, ", "))
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op_insert
type opDelete struct {
	reqID              int32
	fullCollectionName string
	flags              int32
	selector           bsoncore.Document
}

func (d *opDelete) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
}

func decodeDelete(reqID int32, wm []byte) (*opDelete, error) {
	var ok bool
	d := opDelete{
		reqID: reqID,
	}

	_, wm, ok = readi32(wm)
	if !ok {
		return nil, errors.New("malformed delete message: missing zero")
	}

	d.fullCollectionName, wm, ok = readCString(wm)
	if !ok {
		return nil, errors.New("malformed delete message: full collection name")
	}

	d.flags, wm, ok = readi32(wm)
	if !ok {
		return nil, errors.New("malformed delete message: missing OP_DELETE flags")
	}

	d.selector, _, ok = bsoncore.ReadDocument(wm)
	if !ok {
		return nil, errors.New("malformed delete message: selector document")
	}

	return &d, nil
}

func (d *opDelete) OpCode() wiremessage.OpCode {
	return wiremessage.OpDelete
}

func (d *opDelete) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpDelete)
	buffer = appendCString(buffer, d.fullCollectionName)
	buffer = appendi32(buffer, d.flags)
	buffer = append(buffer, d.selector...)
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (d *opDelete) IsIsMaster() bool {
	return false
}

func (d *opDelete) CursorID() (cursorID int64, ok bool) {
	return 0, false
}

func (d *opDelete) RequestID() int32 {
	return d.reqID
}

func (d *opDelete) Error() error {
	return nil
}

func (d *opDelete) Unacknowledged() bool {
	return false
}

func (d *opDelete) CommandAndCollection() (Command, string) {
	return Delete, d.fullCollectionName
}

func (d *opDelete) String() string {
	return fmt.Sprintf("{ OpDelete fullCollectionName: %s, flags: %d, selector: %s }", d.fullCollectionName, d.flags, d.selector.String())
}

// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op_kill_cursors
type opKillCursors struct {
	reqID     int32
	cursorIDs []int64
}

func (k *opKillCursors) TransactionDetails() (*TransactionDetails, bool) {
	return nil, false
}

func decodeKillCursors(reqID int32, wm []byte) (*opKillCursors, error) {
	var ok bool
	k := opKillCursors{
		reqID: reqID,
	}

	_, wm, ok = wiremessage.ReadKillCursorsZero(wm)
	if !ok {
		return nil, errors.New("malformed kill_cursors message: missing zero")
	}

	var numIDs int32
	numIDs, wm, ok = wiremessage.ReadKillCursorsNumberIDs(wm)
	if !ok {
		return nil, errors.New("malformed kill_cursors message: missing number of cursor IDs")
	}

	k.cursorIDs, _, ok = wiremessage.ReadKillCursorsCursorIDs(wm, numIDs)
	if !ok {
		return nil, errors.New("malformed kill_cursors message: missing cursor IDs")
	}

	return &k, nil
}

func (k *opKillCursors) OpCode() wiremessage.OpCode {
	return wiremessage.OpKillCursors
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation_legacy.go#L378-L384
func (k *opKillCursors) Encode(responseTo int32) []byte {
	var buffer []byte
	idx, buffer := wiremessage.AppendHeaderStart(buffer, 0, responseTo, wiremessage.OpKillCursors)
	buffer = wiremessage.AppendKillCursorsZero(buffer)
	buffer = wiremessage.AppendKillCursorsNumberIDs(buffer, int32(len(k.cursorIDs)))
	buffer = wiremessage.AppendKillCursorsCursorIDs(buffer, k.cursorIDs)
	buffer = bsoncore.UpdateLength(buffer, idx, int32(len(buffer[idx:])))
	return buffer
}

func (k *opKillCursors) IsIsMaster() bool {
	return false
}

func (k *opKillCursors) CursorID() (cursorID int64, ok bool) {
	return 0, false
}

func (k *opKillCursors) RequestID() int32 {
	return k.reqID
}

func (k *opKillCursors) Error() error {
	return nil
}

func (k *opKillCursors) Unacknowledged() bool {
	return false
}

func (k *opKillCursors) CommandAndCollection() (Command, string) {
	return Unknown, ""
}

func (k *opKillCursors) String() string {
	return fmt.Sprintf("{ OpKillCursors cursorIDs: %v }", k.cursorIDs)
}

func appendi32(dst []byte, i32 int32) []byte {
	return append(dst, byte(i32), byte(i32>>8), byte(i32>>16), byte(i32>>24))
}

func appendCString(b []byte, str string) []byte {
	b = append(b, str...)
	return append(b, 0x00)
}

func readi32(src []byte) (int32, []byte, bool) {
	if len(src) < 4 {
		return 0, src, false
	}

	return int32(src[0]) | int32(src[1])<<8 | int32(src[2])<<16 | int32(src[3])<<24, src[4:], true
}

func readCString(src []byte) (string, []byte, bool) {
	idx := bytes.IndexByte(src, 0x00)
	if idx < 0 {
		return "", src, false
	}
	return string(src[:idx]), src[idx+1:], true
}
