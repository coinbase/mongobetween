package util

import (
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

func StatsdWithTags(client *statsd.Client, tags []string) (*statsd.Client, error) {
	tags = append(client.Tags, tags...)
	return statsd.CloneWithExtraOptions(client, statsd.WithTags(tags))
}

type StatsdBackgroundGaugeCallback func(name string, tags []string)

func StatsdBackgroundGauge(client *statsd.Client, name string, tags []string) (increment, decrement StatsdBackgroundGaugeCallback) {
	inc := make(chan bool)
	increment = func(name string, tags []string) {
		_ = client.Incr(name, tags, 1)
		inc <- true
	}

	dec := make(chan bool)
	decrement = func(name string, tags []string) {
		_ = client.Incr(name, tags, 1)
		dec <- true
	}

	go func() {
		count := 0
		for {
			select {
			case <-inc:
				count++
			case <-dec:
				count--
			case <-time.After(1 * time.Second):
			}
			_ = client.Gauge(name, float64(count), tags, 1)
		}
	}()

	return
}
