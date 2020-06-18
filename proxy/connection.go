package proxy

import (
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/mongo"
)

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client

	conn   net.Conn
	client *mongo.Mongo
	kill   chan interface{}
	buffer []byte
}

func handleConnection(log *zap.Logger, sd *statsd.Client, conn net.Conn, client *mongo.Mongo, kill chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:    log,
		statsd: sd,
		conn:   conn,
		client: client,
		kill:   kill,
	}
	c.processMessages()
}

func (c *connection) processMessages() {
	for {
		err := c.handleMessage()
		if err != nil {
			if err != io.EOF {
				select {
				case <-c.kill:
					// ignore errors from force shutdown
				default:
					c.log.Error("Error handling message", zap.Error(err))
				}
			}
			return
		}
	}
}

func (c *connection) handleMessage() (err error) {
	isMaster := false
	var reqOpCode, resOpCode wiremessage.OpCode

	defer func(start time.Time) {
		_ = c.statsd.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
			fmt.Sprintf("is_master:%v", isMaster),
			fmt.Sprintf("request_op_code:%v", reqOpCode),
			fmt.Sprintf("response_op_code:%v", resOpCode),
		}, 1)
	}(time.Now())

	var wm []byte
	if wm, err = c.readWireMessage(); err != nil {
		return
	}

	var op mongo.Operation
	if op, err = mongo.Decode(wm); err != nil {
		return
	}

	c.log.Debug("Request", zap.Int32("op_code", int32(op.OpCode())), zap.Int("request_size", len(wm)))

	isMaster = op.IsIsMaster()
	req := &mongo.Message{
		Wm: wm,
		Op: op,
	}
	reqOpCode = op.OpCode()

	var res *mongo.Message
	if res, err = c.roundTrip(req, isMaster); err != nil {
		cursorID, _ := op.CursorID()
		c.log.Error(
			"Round trip error",
			zap.Error(err),
			zap.Int64("cursor_id", cursorID),
			zap.Int32("op_code", int32(reqOpCode)),
		)
		return
	}
	resOpCode = res.Op.OpCode()

	if _, err = c.conn.Write(res.Wm); err != nil {
		return
	}

	c.log.Debug("Response", zap.Int32("op_code", int32(resOpCode)), zap.Int("response_size", len(res.Wm)))
	return
}

func (c *connection) readWireMessage() ([]byte, error) {
	var sizeBuf [4]byte

	_, err := io.ReadFull(c.conn, sizeBuf[:])
	if err != nil {
		return nil, err
	}

	// read the length as an int32
	size := (int32(sizeBuf[0])) | (int32(sizeBuf[1]) << 8) | (int32(sizeBuf[2]) << 16) | (int32(sizeBuf[3]) << 24)
	if int(size) > cap(c.buffer) {
		c.buffer = make([]byte, 0, size)
	}

	buffer := c.buffer[:size]
	copy(buffer, sizeBuf[:])

	_, err = io.ReadFull(c.conn, buffer[4:])
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (c *connection) roundTrip(msg *mongo.Message, isMaster bool) (*mongo.Message, error) {
	if isMaster {
		requestID := msg.Op.RequestID()
		c.log.Debug("Non-proxied ismaster response", zap.Int32("request_id", requestID))
		return mongo.IsMasterResponse(requestID, c.client.TopologyKind())
	}

	c.log.Debug("Proxying request to upstream server", zap.Int("request_size", len(msg.Wm)))
	return c.client.RoundTrip(msg)
}
