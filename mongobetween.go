package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/config"
)

func main() {
	// Initialize Datadog tracer
	initDatadogTracer()
	defer tracer.Stop()

	c := config.ParseFlags()
	run(c)
}

func initDatadogTracer() {
	// Get service name from DD_SERVICE env var or use default
	serviceName := os.Getenv("DD_SERVICE")
	if serviceName == "" {
		serviceName = "mongobetween"
	}

	// Get environment from DD_ENV env var or use default
	env := os.Getenv("DD_ENV")
	if env == "" {
		env = "prod"
	}

	// Get version from DD_VERSION env var or use build info
	version := os.Getenv("DD_VERSION")
	if version == "" {
		if info, ok := debug.ReadBuildInfo(); ok {
			version = info.Main.Version
		}
		if version == "" || version == "(devel)" {
			version = "unknown"
		}
	}

	// Set up tracer options
	opts := []tracer.StartOption{
		tracer.WithService(serviceName),
		tracer.WithEnv(env),
		tracer.WithServiceVersion(version),
	}

	// Handle DD_AGENT_HOST or DD_TRACE_AGENT_URL
	if agentHost := os.Getenv("DD_AGENT_HOST"); agentHost != "" {
		opts = append(opts, tracer.WithAgentAddr(agentHost+":8126"))
	} else if traceURL := os.Getenv("DD_TRACE_AGENT_URL"); traceURL != "" {
		opts = append(opts, tracer.WithAgentAddr(traceURL))
	}

	tracer.Start(opts...)
}

func run(config *config.Config) {
	proxies, err := config.Proxies(config.Logger())
	log := config.Logger()

	if err != nil {
		log.Fatal("Startup error", zap.Error(err))
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
