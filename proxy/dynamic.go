package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-getter"
	"go.uber.org/zap"
)

type Dynamic struct {
	config *DynamicClusters
	mutex  sync.RWMutex
}

type DynamicClusters struct {
	Clusters map[string]DynamicCluster
}

type DynamicCluster struct {
	DisableWrites         bool
	RedirectTo            string
	DualReadFrom          string
	DualReadSamplePercent int
}

func NewDynamic(url string, log *zap.Logger) (*Dynamic, error) {
	d := Dynamic{}
	err := d.start(url, log)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *Dynamic) ForAddress(address string) DynamicCluster {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.config != nil && d.config.Clusters != nil {
		if v, ok := d.config.Clusters[address]; ok {
			return v
		}
	}

	return DynamicCluster{}
}

func (d *Dynamic) start(url string, log *zap.Logger) error {
	if url == "" {
		return nil
	}

	// poll at least once in the main thread
	if err := d.poll(url, log); err != nil {
		return err
	}

	go func() {
		for {
			time.Sleep(10 * time.Second) // TODO make configurable
			if err := d.poll(url, log); err != nil {
				log.Error("Error polling dynamic URL", zap.String("url", url), zap.Error(err))
			}
		}
	}()

	return nil
}

func (d *Dynamic) poll(url string, log *zap.Logger) error {
	f, err := ioutil.TempFile("", "*.json")
	if err != nil {
		return err
	}
	err = f.Close() // Close now, reopen after writing
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close() // need another Close because reopened below
		_ = os.Remove(f.Name())
	}()

	// Copy the input URI to the temporary file before reading. go-getter supports reading from a variety
	// of input URIs, see https://github.com/hashicorp/go-getter.
	pwd, _ := os.Getwd()
	client := &getter.Client{
		Ctx:  context.Background(),
		Src:  url,
		Dst:  f.Name(),
		Pwd:  pwd,
		Mode: getter.ClientModeFile,
	}
	if err := client.Get(); err != nil {
		return err
	}

	f, err = os.Open(f.Name())
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	config := DynamicClusters{}
	if err = json.Unmarshal(body, &config); err != nil {
		return err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	newConfig, _ := json.Marshal(config)
	if d.config == nil {
		log.Info("Loaded dynamic config", zap.String("config", string(newConfig)))
	} else {
		oldConfig, _ := json.Marshal(*d.config)
		if !bytes.Equal(oldConfig, newConfig) {
			log.Info("Updated dynamic config", zap.String("config", string(newConfig)), zap.String("old_config", string(oldConfig)))
		}
	}

	d.config = &config
	return nil
}
