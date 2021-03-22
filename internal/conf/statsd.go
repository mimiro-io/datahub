// Copyright 2021 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conf

import (
	"context"
	"runtime"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func NewStatsD(lc fx.Lifecycle, env *Env, logger *zap.SugaredLogger) (statsd.ClientInterface, error) {
	var client statsd.ClientInterface
	agentEndpoint := env.AgentHost
	if agentEndpoint != "" {
		opt := statsd.WithNamespace("mimiro.datahub.")
		logger.Info("Statsd is configured on: ", agentEndpoint)
		c, err := statsd.New(agentEndpoint, opt)
		if err != nil {
			return nil, err
		}
		client = c
	} else {
		logger.Debug("Using NoOp statsd client")
		client = &statsd.NoOpClient{}
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			env.Logger.Infof("Flushing statsd")
			return client.Flush()
		},
	})

	return client, nil
}

type memoryReporter struct {
	statsd statsd.ClientInterface
	logger *zap.SugaredLogger
}

func NewMemoryReporter(lc fx.Lifecycle, statsd statsd.ClientInterface, logger *zap.SugaredLogger) *memoryReporter {
	mr := &memoryReporter{
		statsd: statsd,
		logger: logger,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			mr.init()
			return nil
		},
	})

	return mr
}

func (mr *memoryReporter) init() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			mr.run()

		}
	}()

}

func (mr *memoryReporter) run() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	_ = mr.statsd.Gauge("mem.alloc", float64(bToMb(m.Alloc)), nil, 1)
	_ = mr.statsd.Gauge("mem.totalAlloc", float64(bToMb(m.TotalAlloc)), nil, 1)
	_ = mr.statsd.Gauge("mem.sys", float64(bToMb(m.Sys)), nil, 1)
	_ = mr.statsd.Gauge("heap.obj", float64(m.Mallocs-m.Frees), nil, 1)
	_ = mr.statsd.Gauge("heap.sys", float64(bToMb(m.HeapSys)), nil, 1)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
