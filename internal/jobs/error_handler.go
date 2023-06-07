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
)

const (
	ErrorHandlerReRun   = "rerun"
	ErrorHandlerReQueue = "requeue"
	ErrorHandlerLog     = "log"
)

var ErrorHandlerTypes = map[string]bool{ErrorHandlerReRun: true, ErrorHandlerReQueue: true, ErrorHandlerLog: true}

type ErrorHandler struct {
	Type       string `json:"errorHandler"` // rerun, requeue, log
	MaxRetries int    `json:"maxRetries"`   // default 1
	RetryDelay int64  `json:"retryDelay"`   // seconds, default 30
	MaxItems   int    `json:"maxItems"`     // 0 = all
}

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
			case "rerun":
				if eh.MaxRetries == 0 {
					eh.MaxRetries = 1
				}
				if eh.RetryDelay == 0 {
					eh.RetryDelay = 30
				}
				eh.RetryDelay = int64(time.Second) * eh.RetryDelay
			}
		}
	}
	return nil
}

func (j *job) handleJobError(err *error) {
	if err == nil || *err == nil {
		j.runner.logger.Debugf("job %v (%v) completed successfully", j.title, j.id)
		return
	}
	if (*err).Error() == "got job interrupt" {
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
			}
		}
	}
}
