package scheduler

import (
	"context"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"sync"
	"time"
)

type runnable interface {
	cron.Job
	Stop(ctx context.Context)
	ID() string
}

const (
	// RunResultFailed is the state of a failed run
	RunResultFailed = "failed"
	// RunResultSuccess is the state of a successful run
	RunResultSuccess = "success"

	TaskStateRunning   = "running"
	TaskStateScheduled = "scheduled"
)

type RunResult struct {
	state     string
	timestame time.Time
}

type schedulable interface {
	runnable
	ImmediateRun() bool
	State() string
}

type schedulableTask struct {
	lock         sync.Mutex
	ticker       *time.Ticker
	logger       *zap.SugaredLogger
	run          func() RunResult
	id           string
	OnStop       func(ctx context.Context) error
	state        string
	sched        cron.Schedule
	immediateRun bool
}

func (s *schedulableTask) ImmediateRun() bool {
	return s.immediateRun
}

func (s *schedulableTask) State() string {
	return s.state
}

func (s *schedulableTask) Stop(ctx context.Context) {
	if s.OnStop != nil {
		s.OnStop(ctx)
	}
}

func (s *schedulableTask) ID() string {
	return s.id
}

func newSchedulableTask(taskId string, immediateRun bool, logger *zap.SugaredLogger, run func() RunResult) *schedulableTask {
	return &schedulableTask{
		id:           taskId,
		immediateRun: immediateRun,
		logger:       logger,
		run:          run,
	}
}

func (s *schedulableTask) Run() {
	s.lock.Lock()
	if s.state == TaskStateRunning {
		s.logger.Infof("Task %s is already running", s.ID())
		s.lock.Unlock()
		return
	}
	s.state = TaskStateRunning
	s.lock.Unlock()

	go func() {
		ts := time.Now()
		defer func() {
			s.lock.Lock()
			s.state = TaskStateScheduled
			s.lock.Unlock()
		}()
		res := s.run()
		//TODO: store result in history
		s.logger.Infof("Task %s completed with state %s in %s. Next run %v", s.ID(), res.state, time.Since(ts), s.sched.Next(time.Now()))
	}()
}
