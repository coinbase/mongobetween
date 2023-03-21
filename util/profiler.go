// Provides continuous profiling for GoLang services in Datadog.
package util

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"

	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

const jitterMaxSec = 60

const (
	errViperIsNil        = "viper instance is nil"
	errEnvironmentNotSet = "Environment not set"
	errVersionNameNotSet = "VersionName not set"
	errProjectNameNotSet = "ProjectName not set"
	errServiceNameNotSet = "ServiceName not set"
)

// See the datadog profiler source for documentation on how these are used
// dd-trace-go.v1@v1.25.0/profiler/options.go
type DatadogProfilerConfig struct {
	Enabled     bool
	ServiceName string
	VersionName string
	ProjectName string
	Environment string
}

// Helper function to load config with viper using some standard env vars. This assumes that Viper
// has already been initialized by the caller.
// It is recommended to use this directly unless there is a good reason to override the configs.
func getDatadogProfilerConfigWithViper(v *viper.Viper) (*DatadogProfilerConfig, error) {
	if v == nil {
		return nil, errors.New(errViperIsNil)
	}

	return &DatadogProfilerConfig{
		Enabled:     v.GetBool("datadog.profiler_enabled"),
		VersionName: v.GetString("codeflow_release_id"),
		ProjectName: v.GetString("codeflow.project_name"),
		Environment: v.GetString("codeflow.config_name"),

		// Create a non-ambiguous service name for the profiles. We can't rely on only the combination
		// of CODEFLOW_PROJECT_NAME and CODEFLOW_SERVICE_NAME as we can have multiple containers on a
		// single EC2 instance. The name of the binary is used to disambiguate this.
		ServiceName: fmt.Sprintf(
			"%s/%s/%s",
			viper.GetString("codeflow.project_name"),
			viper.GetString("codeflow.service_name"),
			filepath.Base(os.Args[0]),
		),
	}, nil
}

type DatadogProfiler struct {
	cfg          *DatadogProfilerConfig
	log          *zap.Logger
	running      bool
	profileTypes []profiler.ProfileType
}

func newDataDogProfiler(
	cfg *DatadogProfilerConfig,
	log *zap.Logger,
	profileTypes ...profiler.ProfileType,
) (*DatadogProfiler, error) {
	if cfg.Environment == "" {
		return nil, errors.New(errEnvironmentNotSet)
	}

	if cfg.VersionName == "" {
		return nil, errors.New(errVersionNameNotSet)
	}

	if cfg.ProjectName == "" {
		return nil, errors.New(errProjectNameNotSet)
	}

	if cfg.ServiceName == "" {
		return nil, errors.New(errServiceNameNotSet)
	}

	if len(profileTypes) == 0 {
		profileTypes = []profiler.ProfileType{
			// HeapProfile reports memory allocation samples; used to monitor current
			// and historical memory usage, and to check for memory leaks.
			profiler.HeapProfile,
			// CPUProfile determines where a program spends its time while actively consuming
			// CPU cycles (as opposed to while sleeping or waiting for I/O).
			profiler.CPUProfile,
			// BlockProfile shows where goroutines block waiting on synchronization primitives
			// (including timer channels). Block profile is not enabled by default.
			profiler.BlockProfile,
			// MutexProfile reports the lock contentions. When you think your CPU is not fully utilized due
			// to a mutex contention, use this profile. Mutex profile is not enabled by default.
			profiler.MutexProfile,
			// GoroutineProfile reports stack traces of all current goroutines
			profiler.GoroutineProfile,
		}
	}
	return &DatadogProfiler{
		cfg:          cfg,
		log:          log,
		profileTypes: profileTypes,
	}, nil
}

func StartDatadogProfiler(log *zap.Logger) *DatadogProfiler {
	cfg, err := getDatadogProfilerConfigWithViper(viper.GetViper())
	if err != nil {
		log.Warn("Cannot get Datadog profiler config", zap.Error(err))
	} else if cfg.Enabled {
		profiler, err := newDataDogProfiler(cfg, log)
		if err != nil {
			log.Warn("Cannot start Datadog profiler", zap.Error(err))
		} else {
			log.Info("Starting Datadog profiler")
			profiler.Run()
			return profiler
		}
	}
	return nil
}

func (a *DatadogProfiler) Run() {
	if a.running {
		a.log.Warn("datadog profiler already running")
		return
	}

	go func() {
		// jitter to prevent profiles from happening on all instances at the same time
		time.Sleep(time.Duration(rand.Intn(jitterMaxSec)) * time.Second) // #nosec

		a.log.Info("datadog profiler started")

		err := profiler.Start(
			profiler.WithService(a.cfg.ServiceName),
			profiler.WithEnv(a.cfg.Environment),
			profiler.WithVersion(a.cfg.VersionName),
			profiler.WithTags(fmt.Sprintf("projectname:%s", a.cfg.ProjectName)),
			profiler.WithProfileTypes(a.profileTypes...),
		)
		if err != nil {
			a.log.Error("error starting datadog profiler", zap.Error(err))
		}
	}()

	a.running = true
}

func (a *DatadogProfiler) Stop() {
	a.log.Info("stopping datadog profiler")
	a.running = false

	profiler.Stop()
}
