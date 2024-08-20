package scheduler

import (
	"context"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
	"time"
)

func NewGCUpdate(logger *zap.SugaredLogger, gc *server.GarbageCollector) schedulable {
	t := newSchedulableTask("scheduled_gc", true, logger, func() RunResult {
		ts := time.Now()
		var err error
		logger.Info("Starting to clean deleted datasets")
		err = gc.Cleandeleted()
		if err != nil {
			logger.Warnf("cleaning of deleted datasets failed: %v", err.Error())
			return RunResult{state: RunResultFailed, timestame: time.Now()}
		} else {
			logger.Infof("Finished cleaning of deleted datasets after %v", time.Since(ts).Round(time.Millisecond))
		}
		ts = time.Now()
		logger.Info("Starting badger gc")
		err = gc.GC()
		if err != nil {
			logger.Warn("badger gc failed: ", err)
			return RunResult{state: RunResultFailed, timestame: time.Now()}
		}
		logger.Infof("Finished badger gc after %v", time.Since(ts).Round(time.Millisecond))
		return RunResult{state: RunResultSuccess, timestame: time.Now()}
	})
	t.OnStop = func(ctx context.Context) error {
		logger.Info("Stopping garbage collector")
		return gc.Stop(ctx)
	}
	return t
}
