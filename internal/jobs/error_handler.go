// Copyright 2023 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"errors"
	"strings"
	"time"

	"github.com/mimiro-io/datahub/internal/server"
)

const (
	ErrorHandlerReRun   = "rerun"
	ErrorHandlerReQueue = "requeue"
	ErrorHandlerLog     = "log"
)

var ErrorHandlerTypes = map[string]bool{ErrorHandlerReRun: true, ErrorHandlerReQueue: true, ErrorHandlerLog: true}

type ErrorHandler struct {
	Type                 string `json:"errorHandler"` // rerun, requeue, log
	MaxRetries           int    `json:"maxRetries"`   // default 1
	RetryDelay           int64  `json:"retryDelay"`   // seconds, default 30
	MaxItems             int    `json:"maxItems"`     // 0 = all
	failingEntityHandler failingEntityHandler
}

func (h *ErrorHandler) init(dsm *server.DsManager) {
	if h.failingEntityHandler != nil && h.Type == ErrorHandlerReQueue {
		feh, ok := h.failingEntityHandler.(*ReQueueFailingEntityHandler)
		if ok {
			feh.dsm = dsm
		}
	}
}

type failingEntityHandler interface {
	handleFailingEntity(runner *Runner, entity *server.Entity, jobId string) error
	reset()
}

type LogFailingEntityHandler struct {
	count    int
	MaxItems int
}

func (l *LogFailingEntityHandler) reset() {
	l.count = 0
}

func (l *LogFailingEntityHandler) handleFailingEntity(runner *Runner, entity *server.Entity, jobId string) error {
	if l.MaxItems > 0 && l.count >= l.MaxItems {
		return MaxItemsExceededError
	}
	l.count = l.count + 1
	runner.logger.Warnf("entity %v failed to process", entity.ID)
	return nil
}

type ReQueueFailingEntityHandler struct {
	count    int
	MaxItems int
	queue    *reQueue
	dsm      *server.DsManager
}

func (r *ReQueueFailingEntityHandler) reset() {
	r.count = 0
	r.queue = nil
}

func (r *ReQueueFailingEntityHandler) handleFailingEntity(runner *Runner, entity *server.Entity, jobId string) error {
	if r.MaxItems > 0 && r.count >= r.MaxItems {
		return MaxItemsExceededError
	}
	r.count = r.count + 1
	runner.logger.Warnf("entity %v failed to process, requeueing", entity.ID)
	if r.queue == nil {
		q, err := newReQueue(jobId, r.dsm)
		if err != nil {
			return err
		}
		r.queue = q
	}
	return r.queue.enQueue(entity)
}

type wrappedTransform struct {
	t                     Transform
	failingEntityHandlers []failingEntityHandler
	jobId                 string
}

type wrappedSink struct {
	s                     Sink
	failingEntityHandlers []failingEntityHandler
	jobId                 string
	lastError             error
	lastProcessed         int
	recursionDepth        int
}

// verifyErrorHandlers checks that the error handlers are valid, and also
// sets default values for the error handlers
func verifyErrorHandlers(trigger JobTrigger) error {
	counts := make(map[string]int)
	if len(trigger.ErrorHandlers) > 0 {
		for _, eh := range trigger.ErrorHandlers {
			// fix casing
			eh.Type = strings.ToLower(eh.Type)

			if _, ok := counts[eh.Type]; ok {
				return errors.New("duplicate error handler: " + eh.Type)
			}
			counts[eh.Type] = 1
			if _, ok := ErrorHandlerTypes[eh.Type]; !ok {
				return errors.New("need to set 'errorHandler'. must be one of: reRun, reQueue, log")
			}

			// set defaults
			switch eh.Type {
			case ErrorHandlerReRun:
				if eh.MaxRetries == 0 {
					eh.MaxRetries = 1
				}
				if eh.RetryDelay == 0 {
					eh.RetryDelay = 30
				}
				eh.RetryDelay = int64(time.Second) * eh.RetryDelay
			case ErrorHandlerLog:
				eh.failingEntityHandler = &LogFailingEntityHandler{MaxItems: eh.MaxItems}
			case ErrorHandlerReQueue:
				eh.failingEntityHandler = &ReQueueFailingEntityHandler{MaxItems: eh.MaxItems}
			default:
				return errors.New("unknown error handler: " + eh.Type)
			}
		}
	}
	return nil
}

// handleJobError is called after a job run has completed. if the run ended with an error, this function
// will schedule a rerun if configured
func (j *job) handleJobError(err *error) {
	if err == nil || *err == nil || *err == MaxItemsExceededError {
		if wrappedSink, isWrapped := j.pipeline.spec().sink.(*wrappedSink); isWrapped {
			if wrappedSink.lastError != nil {
				*err = wrappedSink.lastError
				lastRun := &jobResult{}
				_ = j.runner.store.GetObject(server.JobResultIndex, j.id, lastRun)
				lastRun.LastError = (*err).Error()
				if lastRun.Processed > wrappedSink.lastProcessed {
					wrappedSink.lastProcessed = lastRun.Processed
				}
				lastRun.Processed = wrappedSink.lastProcessed
				_ = j.runner.store.StoreObject(server.JobResultIndex, j.id, lastRun)
				j.runner.logger.Warnw("job %v (%v) completed, but errors occurred: %w", j.title, j.id, *err)
			} else {
				return
			}
		} else {
			j.runner.logger.Debugf("job %v (%v) completed successfully", j.title, j.id)
			return
		}
	}
	if err != nil && *err != nil && (*err).Error() == "got job interrupt" {
		j.runner.logger.Debugf("job %v (%v) interrupted", j.title, j.id)
		return
	}
	if j.errorHandlers != nil {
		for _, eh := range j.errorHandlers {
			if eh.Type == ErrorHandlerReRun {
				if eh.MaxRetries > 0 {
					eh.MaxRetries = eh.MaxRetries - 1
					time.AfterFunc(time.Duration(eh.RetryDelay), func() {
						j.runner.logger.Infof("re-running job %v (%v). Try number %v", j.title, j.id, eh.MaxRetries)
						// j.pipeline.sync(j, ctx)
						j.Run()
					})
				}
				return
			}
		}
	}
}

func (j *job) instrumentErrorHandling() {
	if j.errorHandlers != nil {
		failingEntityHandlers := make([]failingEntityHandler, 0)
		for _, eh := range j.errorHandlers {
			if eh.Type == ErrorHandlerLog || eh.Type == ErrorHandlerReQueue {
				failingEntityHandlers = append(failingEntityHandlers, eh.failingEntityHandler)
			}
		}
		if len(failingEntityHandlers) == 0 {
			return
		}

		if j.pipeline.spec().transform != nil {
			wrapped, isWrapped := j.pipeline.spec().transform.(*wrappedTransform)
			if !isWrapped {
				j.pipeline.spec().transform = &wrappedTransform{
					j.pipeline.spec().transform, failingEntityHandlers,
					j.id,
				}
			} else {
				wrapped.reset()
			}
		}
		wrapped, isWrapped := j.pipeline.spec().sink.(*wrappedSink)
		if !isWrapped {
			j.pipeline.spec().sink = &wrappedSink{
				s: j.pipeline.spec().sink, failingEntityHandlers: failingEntityHandlers, jobId: j.id,
			}
		} else {
			wrapped.reset()
		}
	}
	j.pipeline.spec().source = &reQueuePrependingSource{s: j.pipeline.spec().source, jobId: j.id, dsm: j.dsm}
}

func (w *wrappedTransform) GetConfig() map[string]interface{} {
	return w.t.GetConfig()
}

func (w *wrappedTransform) transformEntities(runner *Runner, entities []*server.Entity, jobTag string) ([]*server.Entity, error) {
	transformedEntities, err := w.t.transformEntities(runner, entities, jobTag)
	//if err != nil {
	//	splitPoint := len(entities) / 2
	//	left := entities[:splitPoint]
	//	leftResult, leftErr := w.transformEntities(runner, left, jobTag)
	//	if leftErr != nil && len(entities) <= 1 {
	//		for _, eh := range w.failingEntityHandlers {
	//			for _, entity := range entities {
	//				err2 := eh.handleFailingEntity(runner, entity, w.jobId)
	//				if err2 != nil {
	//					return nil, err2
	//				}
	//			}
	//		}
	//	}
	//
	//	right := entities[splitPoint:]
	//	rightResult, rightErr := w.transformEntities(runner, right, jobTag)
	//	if rightErr != nil && len(entities) <= 1 {
	//		for _, eh := range w.failingEntityHandlers {
	//			for _, entity := range entities {
	//				err2 := eh.handleFailingEntity(runner, entity, w.jobId)
	//				if err2 != nil {
	//					return nil, err2
	//				}
	//			}
	//		}
	//	}
	//
	//	return append(leftResult, rightResult...), nil
	//}
	return transformedEntities, err
}

func (w *wrappedTransform) getParallelism() int {
	return w.t.getParallelism()
}

func (w *wrappedTransform) reset() {
	for _, eh := range w.failingEntityHandlers {
		eh.reset()
	}
}

func (w *wrappedSink) GetConfig() map[string]interface{} {
	return w.s.GetConfig()
}

var MaxItemsExceededError = errors.New("errorHandler: max items reached")

func (w *wrappedSink) processEntities(runner *Runner, entities []*server.Entity) error {
	// try to run batch
	err := w.s.processEntities(runner, entities)
	if err != nil {
		// if this was a single entity, and it failed, run handles
		if len(entities) <= 1 {
			for _, eh := range w.failingEntityHandlers {
				for _, entity := range entities {
					err2 := eh.handleFailingEntity(runner, entity, w.jobId)
					if err2 != nil {
						if w.lastError == nil {
							w.lastError = err
						}
						return err2
						// return w.lastError
					}
				}
			}
			if !errors.Is(err, MaxItemsExceededError) {
				w.lastError = err
			}
			return nil
		} else {
			// if this was a larger batch, split and recusrse
			w.recursionDepth++
			splitPoint := len(entities) / 2
			left := entities[:splitPoint]
			leftErr := w.processEntities(runner, left)
			if !errors.Is(leftErr, MaxItemsExceededError) {
				right := entities[splitPoint:]
				rightErr := w.processEntities(runner, right)
				if rightErr != nil && !errors.Is(rightErr, MaxItemsExceededError) {
					w.lastError = rightErr
				}
				if errors.Is(rightErr, MaxItemsExceededError) {
					return rightErr
				}
			}
			if leftErr != nil && !errors.Is(leftErr, MaxItemsExceededError) {
				w.lastError = leftErr
			}
			return leftErr
		}
	} else {
		// unset error if this was an unsplit batch without failure
		if w.recursionDepth == 0 {
			w.lastError = nil
		}
	}
	return nil
}

func (w *wrappedSink) startFullSync(runner *Runner) error {
	return w.s.startFullSync(runner)
}

func (w *wrappedSink) endFullSync(runner *Runner) error {
	return w.s.endFullSync(runner)
}

func (w *wrappedSink) reset() {
	w.recursionDepth = 0
	for _, eh := range w.failingEntityHandlers {
		eh.reset()
	}
}
