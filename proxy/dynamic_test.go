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
		  "RedirectTo": ""
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

	dy = d.ForAddress("/var/tmp/mongo.sock")
	assert.False(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "/var/tmp/another.sock")

	dy = d.ForAddress("non-existent")
	assert.False(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "")
}
