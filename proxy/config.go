package proxy

import (
	"sync"
)

// TODO make this better

type Config interface {
	IsWritesDisabled() bool
	IsFailover() bool
}

type NoConfig struct {
}

func (c *NoConfig) IsWritesDisabled() bool {
	return false
}

func (c *NoConfig) IsFailover() bool {
	return false
}

func NewConfig(url string) Config {
	return &config{}
}

type config struct {
	mutex         sync.RWMutex
	disableWrites bool
	failover      bool
}

func (c *config) IsWritesDisabled() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.disableWrites
}

//func (c *config) SetWritesDisabled(v bool) {
//	c.mutex.Lock()
//	defer c.mutex.Unlock()
//	c.disableWrites = v
//}

func (c *config) IsFailover() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.failover
}

//func (c *config) SetFailover(v bool) {
//	c.mutex.Lock()
//	defer c.mutex.Unlock()
//	c.failover = v
//}
