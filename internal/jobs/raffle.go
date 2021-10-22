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

package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

type raffle struct {
	logger       *zap.SugaredLogger
	statsdClient statsd.ClientInterface
	ticketsFull  int
	ticketsIncr  int
	runningJobs  map[string]*runState
	runningMu    sync.Mutex
}

type runState struct {
	started time.Time
	isFull  bool
	isEvent bool
	id      string
	title   string
	ctx     context.Context
	cancel  context.CancelFunc
}

type ticket struct {
	runState *runState
}

func NewRaffle(ticketsFull int, ticketsIncr int, logger *zap.SugaredLogger, statsdClient statsd.ClientInterface) *raffle {
	return &raffle{
		logger:       logger,
		statsdClient: statsdClient,
		ticketsFull:  ticketsFull,
		ticketsIncr:  ticketsIncr,
		runningJobs:  make(map[string]*runState),
		runningMu:    sync.Mutex{},
	}
}

func (r *raffle) borrowTicket(job *job) *ticket {
	tags := []string{
		"application:datahub",
	}
	r.runningMu.Lock()
	defer r.runningMu.Unlock()

	_, ok := r.runningJobs[job.id] // is the job running
	if ok {                        // it is, don't give a ticket
		return nil
	}

	if job.pipeline.isFullSync() {
		if r.ticketsFull > 0 {
			r.ticketsFull--
			_ = r.statsdClient.Gauge("jobs.tickets.full", float64(r.ticketsFull), tags, 1)
			ctx, cancel := context.WithCancel(context.Background())
			state := &runState{
				started: time.Now(),
				isEvent: job.isEvent,
				isFull:  true,
				id:      job.id,
				title:   job.title,
				ctx:     ctx,
				cancel:  cancel,
			}
			r.runningJobs[job.id] = state
			return &ticket{runState: state}
		}
	} else {
		if r.ticketsIncr > 0 {
			r.ticketsIncr--
			_ = r.statsdClient.Gauge("jobs.tickets.incr", float64(r.ticketsIncr), tags, 1)
			ctx, cancel := context.WithCancel(context.Background())
			state := &runState{
				started: time.Now(),
				isEvent: job.isEvent,
				isFull:  false,
				id:      job.id,
				title:   job.title,
				ctx:     ctx,
				cancel:  cancel,
			}
			r.runningJobs[job.id] = state
			return &ticket{runState: state}
		}
	}
	return nil
}

func (r *raffle) returnTicket(ticket *ticket) {
	tags := []string{
		"application:datahub",
	}
	r.runningMu.Lock()
	defer r.runningMu.Unlock()

	delete(r.runningJobs, ticket.runState.id)
	if ticket.runState.isFull {
		r.ticketsFull++
		_ = r.statsdClient.Gauge("jobs.tickets.full", float64(r.ticketsFull), tags, 1)
	} else {
		r.ticketsIncr++
		_ = r.statsdClient.Gauge("jobs.tickets.incr", float64(r.ticketsIncr), tags, 1)
	}

}

func (r *raffle) runningJob(jobid string) *runState {
	state, ok := r.runningJobs[jobid]
	if ok {
		return state
	}
	return nil
}

func (r *raffle) getRunningJobs() map[string]*runState {
	return r.runningJobs
}
