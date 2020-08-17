package mongo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/util"
)

const pingTimeout = 60 * time.Second
const disconnectTimeout = 10 * time.Second

type Mongo struct {
	log    *zap.Logger
	statsd *statsd.Client
	opts   *options.ClientOptions

	mu       sync.RWMutex
	client   *mongo.Client
	topology *topology.Topology
	cursors  *cursorCache

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

	opts = opts.SetPoolMonitor(poolMonitor(sd))

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
		roundTripCtx:    rtCtx,
		roundTripCancel: rtCancel,
	}
	go m.cursorMonitor()

	return &m, nil
}

func poolMonitor(sd *statsd.Client) *event.PoolMonitor {
	checkedOut, checkedIn := util.StatsdBackgroundGauge(sd, "pool.checked_out_connections", []string{})
	opened, closed := util.StatsdBackgroundGauge(sd, "pool.open_connections", []string{})

	return &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			snake := strings.ToLower(regexp.MustCompile("([a-z0-9])([A-Z])").ReplaceAllString(e.Type, "${1}_${2}"))
			name := fmt.Sprintf("pool_event.%s", snake)
			tags := []string{
				fmt.Sprintf("address:%s", e.Address),
				fmt.Sprintf("reason:%s", e.Reason),
			}
			switch e.Type {
			case event.ConnectionCreated:
				opened(name, tags)
			case event.ConnectionClosed:
				closed(name, tags)
			case event.GetSucceeded:
				checkedOut(name, tags)
			case event.ConnectionReturned:
				checkedIn(name, tags)
			default:
				_ = sd.Incr(name, tags, 1)
			}
		},
	}
}

func (m *Mongo) Description() description.Topology {
	return m.topology.Description()
}

func (m *Mongo) cursorMonitor() {
	for {
		_ = m.statsd.Gauge("cursors", float64(m.cursors.count()), []string{}, 1)
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

func (m *Mongo) RoundTrip(msg *Message) (_ *Message, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var addr address.Address
	defer func() {
		if err != nil {
			cursorID, _ := msg.Op.CursorID()
			m.log.Error(
				"Round trip error",
				zap.Error(err),
				zap.Int64("cursor_id", cursorID),
				zap.Int32("op_code", int32(msg.Op.OpCode())),
				zap.String("address", addr.String()),
			)
		}
	}()

	if m.client == nil {
		return nil, errors.New("connection closed")
	}

	// FIXME this assumes that cursorIDs are unique on the cluster, but two servers can have the same cursorID reference different cursors
	requestCursorID, _ := msg.Op.CursorID()
	server, err := m.selectServer(requestCursorID)
	if err != nil {
		return nil, err
	}

	// FIXME transactions should be pinned to servers, similar to cursors above

	conn, err := m.checkoutConnection(server)
	if err != nil {
		return nil, err
	}

	addr = conn.Address()

	defer func() {
		err := conn.Close()
		if err != nil {
			m.log.Error("Error closing Mongo connection", zap.Error(err), zap.String("address", addr.String()))
		}
	}()

	// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.5/x/mongo/driver/operation.go#L369-L371
	ep, ok := server.(driver.ErrorProcessor)
	if !ok {
		return nil, errors.New("server ErrorProcessor type assertion failed")
	}

	wm, err := m.roundTrip(conn, msg.Wm, msg.Op.Unacknowledged())
	if err != nil {
		m.processError(err, ep, addr)
		return nil, err
	}
	if msg.Op.Unacknowledged() {
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
		m.processError(opErr, ep, addr)
	}

	if responseCursorID, ok := op.CursorID(); ok {
		if responseCursorID != 0 {
			m.cursors.add(responseCursorID, server)
		} else if requestCursorID != 0 {
			m.cursors.remove(requestCursorID)
		}
	}

	return &Message{
		Wm: wm,
		Op: op,
	}, nil
}

func (m *Mongo) selectServer(requestCursorID int64) (server driver.Server, err error) {
	defer func(start time.Time) {
		_ = m.statsd.Timing("server_selection", time.Since(start), []string{fmt.Sprintf("success:%v", err == nil)}, 1)
	}(time.Now())

	if requestCursorID != 0 {
		server, ok := m.cursors.peek(requestCursorID)
		if ok {
			return server, nil
		}
	}

	selector := description.CompositeSelector([]description.ServerSelector{
		description.ReadPrefSelector(readpref.Primary()),   // ignored by sharded clusters
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.4/x/mongo/driver/operation.go#L532-L561
func (m *Mongo) roundTrip(conn driver.Connection, req []byte, unacknowledged bool) (res []byte, err error) {
	defer func(start time.Time) {
		tags := []string{
			fmt.Sprintf("address:%s", conn.Address().String()),
			fmt.Sprintf("unacknowledged:%v", unacknowledged),
		}

		_ = m.statsd.Distribution("request_size", float64(len(req)), tags, 1)
		if err == nil && !unacknowledged {
			// There is no response size for unacknowledged writes.
			_ = m.statsd.Distribution("response_size", float64(len(res)), tags, 1)
		}

		roundTripTags := append(tags, fmt.Sprintf("success:%v", err == nil))
		_ = m.statsd.Timing("round_trip", time.Since(start), roundTripTags, 1)
	}(time.Now())

	if err = conn.WriteWireMessage(m.roundTripCtx, req); err != nil {
		return nil, wrapNetworkError(err)
	}

	if unacknowledged {
		return nil, nil
	}

	if res, err = conn.ReadWireMessage(m.roundTripCtx, req[:0]); err != nil {
		return nil, wrapNetworkError(err)
	}

	return res, nil
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}

// Process the error with the given ErrorProcessor, returning true if processing causes the topology to change
func (m *Mongo) processError(err error, ep driver.ErrorProcessor, addr address.Address) {
	last := m.Description()

	ep.ProcessError(err)

	if errorChangesTopology(err) {
		desc := m.Description()

		fields := topologyChangedFields(&last, &desc)
		fields = append(fields, zap.String("address", addr.String()))
		fields = append(fields, zap.Error(err))
		if derr, ok := err.(driver.Error); ok {
			fields = append(fields, zap.Int32("error_code", derr.Code))
			fields = append(fields, zap.Strings("error_labels", derr.Labels))
			fields = append(fields, zap.NamedError("error_wrapped", derr.Wrapped))
		}
		if werr, ok := err.(driver.WriteConcernError); ok {
			fields = append(fields, zap.Int64("error_code", werr.Code))
		}
		m.log.Error("Topology changing error", fields...)
	}
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.5/x/mongo/driver/topology/server.go#L300-L341
func errorChangesTopology(err error) bool {
	if cerr, ok := err.(driver.Error); ok && (cerr.NodeIsRecovering() || cerr.NotMaster()) {
		return true
	}
	if wcerr, ok := err.(driver.WriteConcernError); ok && (wcerr.NodeIsRecovering() || wcerr.NotMaster()) {
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

// see https://github.com/mongodb/mongo-go-driver/blob/v1.3.5/x/mongo/driver/topology/server.go#L605-L625
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
