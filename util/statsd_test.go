package util_test

import (
	"github.com/coinbase/mongobetween/util"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

type mockedWriter struct {
	buffer []byte
	m      sync.RWMutex
	mock.Mock
}

func (w *mockedWriter) Write(data []byte) (n int, err error) {
	w.m.Lock()
	defer w.m.Unlock()
	args := w.Called(data)
	w.buffer = append(w.buffer, data...)
	return args.Int(0), args.Error(1)
}
func (w *mockedWriter) SetWriteTimeout(d time.Duration) error {
	args := w.Called(d)
	return args.Error(0)
}
func (w *mockedWriter) Close() error {
	args := w.Called()
	return args.Error(0)
}
func (w *mockedWriter) String() string {
	w.m.RLock()
	defer w.m.RUnlock()
	return string(w.buffer)
}

func TestStatsdBackgroundGauge(t *testing.T) {
	w := mockedWriter{}
	w.On("Write", mock.Anything).Return(0, nil)
	w.On("SetWriteTimeout", mock.Anything).Return(nil)

	client, err := statsd.NewWithWriter(&w)
	assert.Nil(t, err)

	incr, decr := util.StatsdBackgroundGauge(client, "gauge_name", []string{})
	incr("incr_name", []string{})
	decr("decr_name", []string{})

	time.Sleep(1100 * time.Millisecond)

	err = client.Flush()
	assert.Nil(t, err)

	assert.Contains(t, string(w.String()), "gauge_name:1|g")
	assert.Contains(t, string(w.String()), "gauge_name:0|g")
	assert.Contains(t, string(w.String()), "incr_name:1|c")
	assert.Contains(t, string(w.String()), "decr_name:1|c")
}
