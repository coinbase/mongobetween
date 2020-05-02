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

package lruttl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type simpleStruct struct {
	int
	string
}

type complexStruct struct {
	int
	simpleStruct
}

var getTests = []struct {
	name       string
	keyToAdd   interface{}
	keyToGet   interface{}
	expectedOk bool
}{
	{"string_hit", "myKey", "myKey", true},
	{"string_miss", "myKey", "nonsense", false},
	{"simple_struct_hit", simpleStruct{1, "two"}, simpleStruct{1, "two"}, true},
	{"simple_struct_miss", simpleStruct{1, "two"}, simpleStruct{0, "noway"}, false},
	{"complex_struct_hit", complexStruct{1, simpleStruct{2, "three"}}, complexStruct{1, simpleStruct{2, "three"}}, true},
}

func TestGet(t *testing.T) {
	for _, tt := range getTests {
		lru := New(0, time.Hour)
		lru.Add(tt.keyToAdd, 1234)
		val, ok := lru.Get(tt.keyToGet)
		assert.Equal(t, tt.expectedOk, ok, "%s: cache hit", tt.name)
		if ok {
			assert.Equal(t, 1234, val, "%s: cache value", tt.name)
		}
	}
}

func TestRemove(t *testing.T) {
	lru := New(0, time.Hour)
	lru.Add("myKey", 1234)
	val, ok := lru.Get("myKey")
	assert.True(t, ok)
	assert.Equal(t, 1234, val)

	lru.Remove("myKey")
	_, ok = lru.Get("myKey")
	assert.False(t, ok)
}

func TestTTL(t *testing.T) {
	lru := New(0, time.Millisecond*100)
	lru.Add("myKey", 1234)
	val, ok := lru.Get("myKey")
	assert.True(t, ok)
	assert.Equal(t, 1234, val)

	time.Sleep(2100 * time.Millisecond)
	_, ok = lru.Get("myKey")
	assert.False(t, ok, "TestTTL returned a removed entry")
}

func TestTTLReset(t *testing.T) {
	lru := New(0, time.Second)
	lru.Add("myKey", 1234)
	val, ok := lru.Get("myKey")
	assert.True(t, ok)
	assert.Equal(t, 1234, val)

	time.Sleep(500 * time.Millisecond)
	lru.Add("myKey", 5678)
	val, ok = lru.Get("myKey")
	assert.True(t, ok)
	assert.Equal(t, 5678, val)

	time.Sleep(500 * time.Millisecond)
	val, ok = lru.Get("myKey")
	assert.True(t, ok)
	assert.Equal(t, 5678, val)

	time.Sleep(2100 * time.Millisecond)
	_, ok = lru.Get("myKey")
	assert.False(t, ok, "TestTTLReset returned a removed entry")
}

func TestAutoPrune(t *testing.T) {
	c := New(1, time.Hour)
	c.Add("a", 1)
	_, ok := c.Get("a")
	assert.True(t, ok)

	c.Add("b", 2)
	_, ok = c.Get("a")
	assert.False(t, ok, "TestAutoPrune returned a removed entry")
}
