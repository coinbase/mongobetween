package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/config"
	"github.com/coinbase/mongobetween/util"
)

func main() {
	c := config.ParseFlags()
	run(c)
}

func run(config *config.Config) {
	proxies, err := config.Proxies(config.Logger())
	log := config.Logger()
	if err != nil {
		log.Fatal("Startup error", zap.Error(err))
	}

	profiler := util.StartDatadogProfiler(log)
	if profiler != nil {
		defer func() {
			profiler.Stop()
		}()
	}

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	for _, p := range proxies {
		p := p
		wg.Add(1)
		go func() {
			err := p.Run()
			if err != nil {
				log.Error("Error", zap.Error(err))
			}
			wg.Done()
		}()
	}

	shutdown := func() {
		for _, p := range proxies {
			p.Shutdown()
		}
	}
	kill := func() {
		for _, p := range proxies {
			p.Kill()
		}
	}
	shutdownOnSignal(log, shutdown, kill)

	log.Info("Running")
}

func shutdownOnSignal(log *zap.Logger, shutdownFunc func(), killFunc func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		shutdownAttempted := false
		for sig := range c {
			log.Info("Signal", zap.String("signal", sig.String()))

			if !shutdownAttempted {
				log.Info("Shutting down")
				go shutdownFunc()
				shutdownAttempted = true

				if sig == os.Interrupt {
					time.AfterFunc(1*time.Second, func() {
						fmt.Println("Ctrl-C again to kill incoming connections")
					})
				}
			} else if sig == os.Interrupt {
				log.Warn("Terminating")
				_ = log.Sync() // #nosec
				killFunc()
			}
		}
	}()
}
