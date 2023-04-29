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
	"math"
	"runtime"
	"runtime/metrics"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func NewMetricsClient(lc fx.Lifecycle, env *Env, logger *zap.SugaredLogger) (statsd.ClientInterface, error) {
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
		OnStop: func(_ context.Context) error {
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
		OnStart: func(_ context.Context) error {
			mr.init()
			return nil
		},
	})

	return mr
}

func (mr *memoryReporter) init() {
	descs := metrics.All()
	// Create a sample for each metric.
	samples := make([]metrics.Sample, len(descs))
	for i := range samples {
		samples[i].Name = descs[i].Name
	}

	go func() {
		for {
			time.Sleep(15 * time.Second)
			mr.run(samples)
		}
	}()
}

func (mr *memoryReporter) run(samples []metrics.Sample) {
	// Sample the metrics. Re-use the samples slice if you can!
	metrics.Read(samples)

	// Iterate over all results.
	for _, sample := range samples {
		// Pull out the name and value.
		name, value := strings.ReplaceAll(strings.ReplaceAll(sample.Name, "/", ".")[1:], ":", "."), sample.Value

		// Handle each sample.
		switch value.Kind() {
		case metrics.KindUint64:
			_ = mr.statsd.Gauge(name, float64(value.Uint64()), nil, 1)
		case metrics.KindFloat64:
			_ = mr.statsd.Gauge(name, value.Float64(), nil, 1)
		case metrics.KindFloat64Histogram:
			// The histogram may be quite large, so let's just pull out
			// a crude estimate for the median for the sake of this example.
			//val := medianBucket(value.Float64Histogram())
			//_ = mr.statsd.Histogram(name+".hist", val, nil, 1)
			//_ = mr.statsd.Gauge(name, val, nil, 1)
			val := getHistogram(value.Float64Histogram())
			_ = mr.statsd.Gauge(name+".avg", val.avg, nil, 1)
			_ = mr.statsd.Count(name+".count", val.count, nil, 1)
			_ = mr.statsd.Gauge(name+".median", val.median, nil, 1)
			_ = mr.statsd.Gauge(name+".max", val.max, nil, 1)
		case metrics.KindBad:
			// This should never happen because all metrics are supported
			// by construction.
			mr.logger.Error("bug in runtime/metrics package!")
		default:
			// This may happen as new metrics get added.
			//
			// The safest thing to do here is to simply log it somewhere
			// as something to look into, but ignore it for now.
			// In the worst case, you might temporarily miss out on a new metric.
			mr.logger.Warnf("%s: unexpected metric Kind: %v\n", name, value.Kind())
		}
	}

	// still support old metrics until new is in place+
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

type histogram struct {
	avg        float64
	count      int64
	median     float64
	percentile float64
	max        float64
}

func getHistogram(h *metrics.Float64Histogram) histogram {
	count := uint64(0)
	for i := range h.Counts {
		count += h.Counts[i]
	}
	max := h.Buckets[len(h.Buckets)-1]
	if max == math.Inf(1) { // slight optimization
		max = h.Buckets[len(h.Buckets)-2]
	}

	median := medianBucket(h)
	avg := count / uint64(len(h.Counts))

	return histogram{
		avg:    float64(avg),
		max:    max,
		count:  int64(count),
		median: median,
	}
}

func medianBucket(h *metrics.Float64Histogram) float64 {
	total := uint64(0)
	for _, count := range h.Counts {
		total += count
	}
	thresh := total / 2
	total = 0
	for i, count := range h.Counts {
		total += count
		if total >= thresh {
			return h.Buckets[i]
		}
	}
	return 0.0
}
