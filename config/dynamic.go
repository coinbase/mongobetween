package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

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
	DisableWrites bool
	RedirectTo    string
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

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	// poll at least once in the main thread
	if err := d.poll(req); err != nil {
		return err
	}

	go func() {
		for {
			time.Sleep(10 * time.Second) // TODO make configurable
			if err := d.poll(req); err != nil {
				log.Error("Error polling dynamic URL", zap.String("url", url), zap.Error(err))
			}
		}
	}()

	return nil
}

func (d *Dynamic) poll(req *http.Request) error {
	client := http.Client{
		Timeout: time.Second * 2, // Timeout after 2 seconds
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("dynamic config returned http error: %d", res.StatusCode)
	}

	defer func() {
		_ = res.Body.Close()
	}()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	config := DynamicClusters{}
	if err = json.Unmarshal(body, &config); err != nil {
		return err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.config = &config
	return nil
}
