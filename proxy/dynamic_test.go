package proxy

import (
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDynamic(t *testing.T) {
	json := `{
	  "Clusters": {
		":12345": {
		  "DisableWrites": true,
		  "RedirectTo": "",
		  "DualReadFrom": "/var/tmp/dual_read.sock",
		  "DualReadSamplePercent": 51
		},
		"/var/tmp/mongo.sock": {
		  "DisableWrites": false,
		  "RedirectTo": "/var/tmp/another.sock"
		}
	  }
	}`
	f, err := ioutil.TempFile("", "*.json")
	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(f.Name())
	}()
	_, err = f.Write([]byte(json))
	assert.Nil(t, err)
	err = f.Close()
	assert.Nil(t, err)

	d, err := NewDynamic(f.Name(), zap.L())
	assert.Nil(t, err)

	dy := d.ForAddress(":12345")
	assert.True(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "")
	assert.Equal(t, dy.DualReadFrom, "/var/tmp/dual_read.sock")
	assert.Equal(t, dy.DualReadSamplePercent, 51)

	dy = d.ForAddress("/var/tmp/mongo.sock")
	assert.False(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "/var/tmp/another.sock")
	assert.Equal(t, dy.DualReadFrom, "")
	assert.Equal(t, dy.DualReadSamplePercent, 0)

	dy = d.ForAddress("non-existent")
	assert.False(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "")
	assert.Equal(t, dy.DualReadFrom, "")
	assert.Equal(t, dy.DualReadSamplePercent, 0)
}
