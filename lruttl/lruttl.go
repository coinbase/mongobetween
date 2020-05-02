/*
Copyright 2013 Google Inc.
Copyright 2020 Coinbase

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package lruttl implements an LRU cache with TTL
package lruttl

import (
	"container/list"
	"sync"
	"time"
)

// LRU cache with TTL. It is safe for concurrent access.
//
// Based on https://github.com/golang/groupcache/tree/master/lru. Adds:
//  - locking for concurrent access
//  - expiry of records
//  - background cleanup of expired records

type Cache struct {
	maxEntries int
	expiry     time.Duration
	ll         *list.List
	cache      map[interface{}]*list.Element
	mu         sync.RWMutex
}

// New creates a new Cache.
// If maxEntries is zero, the cache has no limit.
// if expiry is zero, records do not expire.
func New(maxEntries int, expiry time.Duration) *Cache {
	c := Cache{
		maxEntries: maxEntries,
		expiry:     expiry,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
	if expiry != 0 {
		go c.expireLoop()
	}
	return &c
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type Key interface{}

type entry struct {
	key    Key
	value  interface{}
	expiry int64
}

func newEntry(key Key, value interface{}, expiry time.Duration) *entry {
	return &entry{
		key:    key,
		value:  value,
		expiry: time.Now().Add(expiry).UnixNano(),
	}
}

func (e *entry) expiryTime() time.Time {
	return time.Unix(0, e.expiry)
}

func (e *entry) expired() bool {
	return time.Now().UnixNano() > e.expiry
}

func (e *entry) touch(expiry time.Duration) {
	e.expiry = time.Now().Add(expiry).UnixNano()
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.len()
}

// Oldest returns the time of the oldest item in the cache.
func (c *Cache) Oldest() (t time.Time, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.oldest()
}

// Peek looks up a key's value from the cache.
func (c *Cache) Peek(key Key) (value interface{}, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.peek(key)
}

// Get looks up a key's value from the cache, bringing it to the front.
func (c *Cache) Get(key Key) (value interface{}, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(key)
}

// Add adds a value to the cache.
func (c *Cache) Add(key Key, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.add(key, value)
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.remove(key)
}

// Clear purges all stored items from the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clear()
}

// background TTL of records

func (c *Cache) expireLoop() {
	for {
		time.Sleep(1 * time.Second)
		if c.oldestExpired() {
			for c.removeOldestExpired() {
			}
		}
	}
}

func (c *Cache) oldestExpired() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ele := c.ll.Back()
	return ele != nil && ele.Value.(*entry).expired()
}

func (c *Cache) removeOldestExpired() (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ele := c.ll.Back()
	if ele != nil && ele.Value.(*entry).expired() {
		c.removeElement(ele)
		return true
	}
	return
}

// private, concurrent unsafe methods

func (c *Cache) len() int {
	return c.ll.Len()
}

func (c *Cache) oldest() (t time.Time, ok bool) {
	if c.len() == 0 {
		return
	}
	return c.ll.Back().Value.(*entry).expiryTime().Add(-c.expiry), true
}

func (c *Cache) peek(key Key) (value interface{}, ok bool) {
	if ele, hit := c.cache[key]; hit {
		return ele.Value.(*entry).value, true
	}
	return
}

func (c *Cache) get(key Key) (value interface{}, ok bool) {
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		e := ele.Value.(*entry)
		e.touch(c.expiry)
		return e.value, true
	}
	return
}

func (c *Cache) add(key Key, value interface{}) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		e := ele.Value.(*entry)
		e.touch(c.expiry)
		e.value = value
		return
	}
	ele := c.ll.PushFront(newEntry(key, value, c.expiry))
	c.cache[key] = ele
	if c.maxEntries != 0 && c.ll.Len() > c.maxEntries {
		c.removeOldest()
	}
}

func (c *Cache) remove(key Key) {
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

func (c *Cache) removeOldest() (ok bool) {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
		return true
	}
	return
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
}

func (c *Cache) clear() {
	c.cache = make(map[interface{}]*list.Element)
	c.ll = list.New()
}
