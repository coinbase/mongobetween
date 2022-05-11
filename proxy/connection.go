package proxy

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"
	"runtime/debug"
	"time"

	"github.com/DataDog/datadog-go/statsd"
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
	var tags []string

	var wm []byte
	if wm, err = c.readWireMessage(); err != nil {
		return
	}

	defer func(start time.Time) {
		tags := append(tags, fmt.Sprintf("success:%v", err == nil))
		_ = c.statsd.Timing("handle_message", time.Since(start), tags, 1)
	}(time.Now())

	var op mongo.Operation
	if op, err = mongo.Decode(wm); err != nil {
		return
	}

	isMaster := op.IsIsMaster()
	command, collection := op.CommandAndCollection()
	unacknowledged := op.Unacknowledged()
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
	reqCopy, err := mongo.CopyMessage(req)
	if err != nil {
		return
	}

	var res *mongo.Message
	if res, err = c.roundTrip(req, isMaster, tags); err != nil {
		return
	}

	if unacknowledged {
		c.log.Debug("Unacknowledged request")
		return
	}

	tags = append(
		tags,
		fmt.Sprintf("response_op_code:%v", res.Op.OpCode()),
	)

	if _, err = c.conn.Write(res.Wm); err != nil {
		return
	}

	c.roundTripWithDualCursor(reqCopy, res, isMaster, tags)

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

func (c *connection) roundTrip(msg *mongo.Message, isMaster bool, tags []string) (*mongo.Message, error) {
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

	return client.RoundTrip(msg, tags)
}

func (c *connection) roundTripWithDualCursor(msg *mongo.Message, primaryRes *mongo.Message, isMaster bool, tags []string) {
	dynamic := c.dynamic.ForAddress(c.address)
	if dynamic.DualReadFrom != "" {
		command, str := msg.Op.CommandAndCollection()
		c.log.Info("Checking for read ", zap.Bool("is_read", mongo.IsRead(command)), zap.String("command", string(command)), zap.String("string result", str))

		bigint, err := rand.Int(rand.Reader, big.NewInt(100))
		if err != nil {
			bigint = big.NewInt(100)
		}
		if mongo.IsRead(command) && int(bigint.Int64()) <= dynamic.DualReadSamplePercent {
			dualReadClient := c.mongoLookup(dynamic.DualReadFrom)

			var dualReadMessage *mongo.Message
			var err error
			if isMaster {
				requestID := msg.Op.RequestID()
				c.log.Debug("Non-proxied ismaster response", zap.Int32("request_id", requestID))
				_, _ = mongo.IsMasterResponse(requestID, dualReadClient.Description().Kind)
				// Don't compare isMaster queries
				return
			}

			primaryCursor, _ := primaryRes.Op.CursorID()
			tags = append(tags, "dual_read:true")
			dualReadMessage, err = dualReadClient.RoundTripWithDualCursor(msg, tags, primaryCursor)

			if err != nil {
				c.log.Error("Error dual reading: ", zap.Error(err))
			}

			primSection := mongo.MustOpMsgCursorSection(primaryRes.Op)
			dualSection := mongo.MustOpMsgCursorSection(dualReadMessage.Op)

			if bytes.Equal(primSection, dualSection) {
				c.log.Info("Dual reads match", zap.String("real_socket", c.address), zap.String("test_socket", dynamic.DualReadFrom))
			} else {
				c.log.Info("Dual reads mismatch", zap.String("real_socket", c.address), zap.String("test_socket", dynamic.DualReadFrom))
			}
		}
	}
}
