package scheduler

import (
	"context"
	"sync"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type Scheduler struct {
	store   store.BadgerStore
	gc      *server.GarbageCollector
	logger  *zap.SugaredLogger
	stopped bool
	cron    *cron.Cron
}

func (s *Scheduler) Start(conf *conf.Config) error {
	s.cron.AddJob("0 2 * * *", NewStatisticsUpdater(s.logger, s.store)) //daily at 2am
	s.cron.AddJob("0 19 * * *", NewGCUpdate(s.logger, s.gc))            //daily at 7pm
	if conf.NamespaceCleanupMode != "" && conf.NamespaceCleanupMode != "off" && conf.NamespaceCleanupMode != "disabled" {
		s.cron.AddJob("0 14 * * 0", NewNamespaceCleaner(s.logger, s.store, conf.NamespaceCleanupMode == "delete")) //sundays at 2pm
	} else {
		s.logger.Info("Namespace cleanup is disabled")
	}
	s.cron.Start()
	for _, e := range s.cron.Entries() {
		task := e.Job.(schedulable)
		if task.ImmediateRun() {
			go func() {
				s.logger.Infof("Running task %s immediately", task.ID())
				task.Run()
			}()
		}
		task.(*schedulableTask).sched = e.Schedule
		s.logger.Infof("Scheduled task %s to run at %s", e.Job.(schedulable).ID(), e.Next)
	}
	return nil
}

func (s *Scheduler) Stop(ctx context.Context) {
	if s.stopped {
		return
	}
	s.stopped = true
	s.cron.Stop()
	for _, task := range s.cron.Entries() {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			task.Job.(schedulable).Stop(ctx)
		}()
		wg.Wait()
	}
}

func NewScheduler(logger *zap.SugaredLogger, gc *server.GarbageCollector, store store.BadgerStore) *Scheduler {
	return &Scheduler{
		logger: logger,
		gc:     gc,
		store:  store,
		cron:   cron.New(),
	}
}
