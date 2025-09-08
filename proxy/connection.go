package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/mongo"
)

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client

	address string
	conn    net.Conn
	kill    chan interface{}
	buffer  []byte

	mongoLookup MongoLookup
	dynamic     *Dynamic
}

func handleConnection(log *zap.Logger, sd *statsd.Client, address string, conn net.Conn, mongoLookup MongoLookup, dynamic *Dynamic, kill chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:    log,
		statsd: sd,

		address: address,
		conn:    conn,
		kill:    kill,

		mongoLookup: mongoLookup,
		dynamic:     dynamic,
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
	// Create parent span for message handling
	span, ctx := tracer.StartSpanFromContext(context.Background(), "mongobetween.handle_message")
	defer func() {
		if err != nil && err != io.EOF {
			span.SetTag("error", true)
			span.SetTag("error.msg", err.Error())
		}
		span.Finish()
	}()

	// Set basic span tags
	span.SetTag("component", "mongobetween")
	span.SetTag("peer.address", c.address)
	if remoteAddr := c.conn.RemoteAddr(); remoteAddr != nil {
		span.SetTag("peer.hostname", remoteAddr.String())
	}

	var tags []string

	defer func(start time.Time) {
		tags := append(tags, fmt.Sprintf("success:%v", err == nil))
		_ = c.statsd.Timing("handle_message", time.Since(start), tags, 1)
	}(time.Now())

	var wm []byte
	if wm, err = c.readWireMessage(); err != nil {
		return
	}

	// Add request size to span
	span.SetTag("mongodb.request.size", len(wm))

	var op mongo.Operation
	if op, err = mongo.Decode(wm); err != nil {
		return
	}

	isMaster := op.IsIsMaster()
	command, collection := op.CommandAndCollection()
	unacknowledged := op.Unacknowledged()

	// Add MongoDB-specific span tags (avoid PII)
	span.SetTag("mongodb.opcode", int32(op.OpCode()))
	span.SetTag("mongodb.command", string(command))
	if collection != "" {
		span.SetTag("mongodb.collection", collection)
	}
	span.SetTag("mongodb.is_master", isMaster)
	span.SetTag("mongodb.unacknowledged", unacknowledged)

	tags = append(
		tags,
		fmt.Sprintf("request_op_code:%v", op.OpCode()),
		fmt.Sprintf("is_master:%v", isMaster),
		fmt.Sprintf("command:%s", string(command)),
		fmt.Sprintf("collection:%s", collection),
		fmt.Sprintf("unacknowledged:%v", unacknowledged),
	)
	c.log.Debug(
		"Request",
		zap.Int32("op_code", int32(op.OpCode())),
		zap.Bool("is_master", isMaster),
		zap.String("command", string(command)),
		zap.String("collection", collection),
		zap.Int("request_size", len(wm)),
	)

	req := &mongo.Message{
		Wm: wm,
		Op: op,
	}
	var res *mongo.Message
	if res, err = c.roundTrip(ctx, req, isMaster, tags); err != nil {
		return
	}

	if unacknowledged {
		c.log.Debug("Unacknowledged request")
		return
	}

	// Add response size to span
	if res != nil {
		span.SetTag("mongodb.response.size", len(res.Wm))
		span.SetTag("mongodb.response.opcode", int32(res.Op.OpCode()))
	}

	tags = append(
		tags,
		fmt.Sprintf("response_op_code:%v", res.Op.OpCode()),
	)

	if _, err = c.conn.Write(res.Wm); err != nil {
		return
	}

	c.log.Debug(
		"Response",
		zap.Int32("op_code", int32(res.Op.OpCode())),
		zap.Int("response_size", len(res.Wm)),
	)
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

func (c *connection) roundTrip(ctx context.Context, msg *mongo.Message, isMaster bool, tags []string) (*mongo.Message, error) {
	dynamic := c.dynamic.ForAddress(c.address)
	if dynamic.DisableWrites {
		command, _ := msg.Op.CommandAndCollection()
		if mongo.IsWrite(command) {
			return nil, fmt.Errorf("writes are disabled for address: %s", c.address)
		}
	}

	redirectTo := dynamic.RedirectTo
	if redirectTo == "" {
		redirectTo = c.address
	}
	client := c.mongoLookup(redirectTo)
	if client == nil {
		return nil, fmt.Errorf("mongo client not found for address: %s", c.address)
	}

	if isMaster {
		requestID := msg.Op.RequestID()
		c.log.Debug("Non-proxied ismaster response", zap.Int32("request_id", requestID))
		return mongo.IsMasterResponse(requestID, client.Description().Kind)
	}

	return client.RoundTrip(ctx, msg, tags)
}
