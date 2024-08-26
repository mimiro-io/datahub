package scheduler

import (
	"context"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"sync"
)

type Scheduler struct {
	store   store.BadgerStore
	gc      *server.GarbageCollector
	logger  *zap.SugaredLogger
	stopped bool
	cron    *cron.Cron
}

func (s *Scheduler) Start() error {
	s.cron.AddJob("0 19 * * *", NewStatisticsUpdater(s.logger, s.store))
	s.cron.AddJob("0 2 * * *", NewGCUpdate(s.logger, s.gc))
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
