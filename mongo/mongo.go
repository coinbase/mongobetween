package mongo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"
)

const pingTimeout = 60 * time.Second
const disconnectTimeout = 10 * time.Second

type Mongo struct {
	log    *zap.Logger
	statsd *statsd.Client
	opts   *options.ClientOptions

	mu           sync.RWMutex
	client       *mongo.Client
	topology     *topology.Topology
	cursors      *cursorCache
	transactions *transactionCache

	roundTripCtx    context.Context
	roundTripCancel func()
}

func extractTopology(c *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(c).Elem()
	d := e.FieldByName("deployment")
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func Connect(log *zap.Logger, sd *statsd.Client, opts *options.ClientOptions, ping bool) (*Mongo, error) {
	// timeout shouldn't be hit if ping == false, as Connect doesn't block the current goroutine
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	var err error
	log.Info("Connect")
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if ping {
		log.Info("Ping")
		err = c.Ping(ctx, readpref.Primary())
		if err != nil {
			return nil, err
		}
		log.Info("Pong")
	}

	t := extractTopology(c)
	go topologyMonitor(log, t)

	rtCtx, rtCancel := context.WithCancel(context.Background())
	m := Mongo{
		log:             log,
		statsd:          sd,
		opts:            opts,
		client:          c,
		topology:        t,
		cursors:         newCursorCache(),
		transactions:    newTransactionCache(),
		roundTripCtx:    rtCtx,
		roundTripCancel: rtCancel,
	}
	go m.cacheMonitor()

	return &m, nil
}

func (m *Mongo) Description() description.Topology {
	return m.topology.Description()
}

func (m *Mongo) cacheGauge(name string, count float64) {
	_ = m.statsd.Gauge(name, count, []string{}, 1)
}

func (m *Mongo) cacheMonitor() {
	for {
		m.cacheGauge("cursors", float64(m.cursors.count()))
		m.cacheGauge("transactions", float64(m.transactions.count()))
		time.Sleep(1 * time.Second)
	}
}

func (m *Mongo) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		// already closed
		return
	}

	m.roundTripCancel()

	m.log.Info("Disconnect")
	ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
	defer cancel()
	err := m.client.Disconnect(ctx)
	m.client = nil
	if err != nil {
		m.log.Info("Error disconnecting", zap.Error(err))
	}
}

func (m *Mongo) RoundTrip(msg *Message, tags []string) (_ *Message, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var addr address.Address
	defer func() {
		if err != nil {
			cursorID, _ := msg.Op.CursorID()
			command, collection := msg.Op.CommandAndCollection()
			m.log.Error(
				"Round trip error",
				zap.Error(err),
				zap.Int64("cursor_id", cursorID),
				zap.Int32("op_code", int32(msg.Op.OpCode())),
				zap.String("address", addr.String()),
				zap.String("command", string(command)),
				zap.String("collection", collection),
			)
		}
	}()

	if m.client == nil {
		return nil, errors.New("connection closed")
	}

	// CursorID is pinned to a server by CursorID-collection name key
	// Transaction is pinned to a server by the issued lsid
	requestCursorID, _ := msg.Op.CursorID()
	requestCommand, collection := msg.Op.CommandAndCollection()
	txnDetails := msg.Op.TransactionDetails()
	readPref, _ := msg.Op.ReadPref()

	var conn driver.Connection
	var server driver.Server

	// Check for a pinned server based on current transaction lsid first
	if txnDetails != nil {
		var ok bool

		conn, ok = m.transactions.peek(txnDetails.LsID)
		if ok {
			m.log.Debug("found cached transaction", zap.String("lsid", fmt.Sprintf("%+v", txnDetails)))
		}
	} else if requestCursorID != 0 {
		var ok bool

		conn, ok = m.cursors.peek(requestCursorID, collection)
		if ok {
			m.log.Debug("Cached cursorID has been found", zap.Int64("cursor", requestCursorID), zap.String("collection", collection))
		}
	}

	if conn == nil {
		server, err := m.selectServer(collection, readPref)
		if err != nil {
			return nil, err
		}

		conn, err = m.checkoutConnection(server)
		if err != nil {
			return nil, err
		}
	}

	addr = conn.Address()
	tags = append(
		tags,
		fmt.Sprintf("address:%s", conn.Address().String()),
	)

	// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L430-L432
	var errorProcessor driver.ErrorProcessor
	if server != nil {
		var ok bool

		errorProcessor, ok = server.(driver.ErrorProcessor)
		if !ok {
			return nil, errors.New("server ErrorProcessor type assertion failed")
		}
	}

	unacknowledged := msg.Op.Unacknowledged()
	wm, err := m.roundTrip(conn, msg.Wm, unacknowledged, tags)
	if err != nil {
		m.processError(err, errorProcessor, addr, conn)
		return nil, err
	}
	if unacknowledged {
		return &Message{}, nil
	}

	op, err := Decode(wm)
	if err != nil {
		return nil, err
	}

	// check if an error is returned in the server response
	opErr := op.Error()
	if opErr != nil {
		// process the error, but don't return it as we still want to forward the response to the client
		m.processError(opErr, errorProcessor, addr, conn)
	}

	responseCursorID, ok := op.CursorID()

	defer func() {
		// If we haven't opened a cursor and aren't in a transaction, then close
		// the connection.
		if conn != nil && txnDetails == nil && responseCursorID == 0 {
			if err := conn.Close(); err != nil {
				m.log.Error("Error closing Mongo connection", zap.Error(err), zap.String("address", addr.String()))
			}
		}
	}()

	if ok {
		if responseCursorID != 0 {
			m.cursors.add(responseCursorID, collection, conn)
		} else if requestCursorID != 0 {
			// If the response cursor id is zero and the request cursor id is
			// non-zero, then we've exhausted the cursor and so should close and unpin
			// the connection.
			m.cursors.remove(requestCursorID, collection)

			err := conn.Close()
			if err != nil {
				m.log.Error("Error closing Mongo connection", zap.Error(err), zap.String("address", addr.String()))
			}
		}
	}

	if txnDetails != nil {
		if txnDetails.IsStartTransaction {
			m.transactions.add(txnDetails.LsID, conn)
		} else {
			if requestCommand == AbortTransaction || requestCommand == CommitTransaction {
				m.log.Debug("Removing transaction from the cache", zap.String("reqCommand", string(requestCommand)))
				m.transactions.remove(txnDetails.LsID)

				err := conn.Close()
				if err != nil {
					m.log.Error("Error closing Mongo connection", zap.Error(err), zap.String("address", addr.String()))
				}
			}
		}
	}

	return &Message{
		Wm: wm,
		Op: op,
	}, nil
}

func (m *Mongo) selectServer(collection string, readPref *readpref.ReadPref) (server driver.Server, err error) {
	defer func(start time.Time) {
		_ = m.statsd.Timing("server_selection", time.Since(start), []string{fmt.Sprintf("success:%v", err == nil)}, 1)
	}(time.Now())
	// Select a server
	selector := description.CompositeSelector([]description.ServerSelector{
		description.ReadPrefSelector(readPref),             // ignored by sharded clusters
		description.LatencySelector(15 * time.Millisecond), // default localThreshold for the client
	})
	return m.topology.SelectServer(m.roundTripCtx, selector)
}

func (m *Mongo) checkoutConnection(server driver.Server) (conn driver.Connection, err error) {
	defer func(start time.Time) {
		addr := ""
		if conn != nil {
			addr = conn.Address().String()
		}
		_ = m.statsd.Timing("checkout_connection", time.Since(start), []string{
			fmt.Sprintf("address:%s", addr),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	conn, err = server.Connection(m.roundTripCtx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L664-L681
func (m *Mongo) roundTrip(conn driver.Connection, req []byte, unacknowledged bool, tags []string) (res []byte, err error) {
	defer func(start time.Time) {
		tags = append(tags, fmt.Sprintf("success:%v", err == nil))

		_ = m.statsd.Distribution("request_size", float64(len(req)), tags, 1)
		if err == nil && !unacknowledged {
			// There is no response size for unacknowledged writes.
			_ = m.statsd.Distribution("response_size", float64(len(res)), tags, 1)
		}

		_ = m.statsd.Timing("round_trip", time.Since(start), tags, 1)
	}(time.Now())

	if err = conn.WriteWireMessage(m.roundTripCtx, req); err != nil {
		return nil, wrapNetworkError(err)
	}

	if unacknowledged {
		return nil, nil
	}

	if res, err = conn.ReadWireMessage(m.roundTripCtx); err != nil {
		return nil, wrapNetworkError(err)
	}

	return res, nil
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}

// Process the error with the given ErrorProcessor, returning true if processing causes the topology to change
func (m *Mongo) processError(err error, ep driver.ErrorProcessor, addr address.Address, conn driver.Connection) {
	last := m.Description()

	// gather fields for logging
	fields := []zap.Field{
		zap.String("address", addr.String()),
		zap.Error(err),
	}
	if derr, ok := err.(driver.Error); ok {
		fields = append(fields, zap.Int32("error_code", derr.Code))
		fields = append(fields, zap.Strings("error_labels", derr.Labels))
		fields = append(fields, zap.NamedError("error_wrapped", derr.Wrapped))
	}
	if werr, ok := err.(driver.WriteConcernError); ok {
		fields = append(fields, zap.Int64("error_code", werr.Code))
	}

	// process the error
	if ep != nil {
		ep.ProcessError(err, conn)
	}

	// log if the error changed the topology
	if errorChangesTopology(err) {
		desc := m.Description()

		fields = append(fields, topologyChangedFields(&last, &desc)...)
		m.log.Error("Topology changing error", fields...)
	}
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/topology/server.go#L432-L505
func errorChangesTopology(err error) bool {
	if cerr, ok := err.(driver.Error); ok && (cerr.NodeIsRecovering() || cerr.NotPrimary()) {
		return true
	}
	if wcerr, ok := err.(driver.WriteConcernError); ok && (wcerr.NodeIsRecovering() || wcerr.NotPrimary()) {
		return true
	}

	wrappedConnErr := unwrapConnectionError(err)
	if wrappedConnErr == nil {
		return false
	}

	// Ignore transient timeout errors.
	if netErr, ok := wrappedConnErr.(net.Error); ok && netErr.Timeout() {
		return false
	}
	if wrappedConnErr == context.Canceled || wrappedConnErr == context.DeadlineExceeded {
		return false
	}

	return true
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/topology/server.go#L949-L969
func unwrapConnectionError(err error) error {
	connErr, ok := err.(topology.ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	driverErr, ok := err.(driver.Error)
	if !ok || !driverErr.NetworkError() {
		return nil
	}

	connErr, ok = driverErr.Wrapped.(topology.ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	return nil
}
