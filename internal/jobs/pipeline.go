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
	"errors"

	"github.com/mimiro-io/datahub/internal/server"
)

const defaultBatchSize = 10000

type Pipeline interface {
	// sync is what the job.Run function eventually calls to run the pipeline. It takes a reference to the job that is running,
	// but it also takes a cancellable context. This context is created in the ticket, and is kept with the running runState.
	// If the job is cancelled, the ctx.Done gets triggered, and the part of the code that is listening for that will return
	// an error to stop the pipeline.
	sync(job *job, ctx context.Context) error
	spec() PipelineSpec
	isFullSync() bool
}
type PipelineSpec struct {
	source    Source
	sink      Sink
	transform Transform
	batchSize int
}
type FullSyncPipeline struct{ PipelineSpec }
type IncrementalPipeline struct{ PipelineSpec }

func (pipeline *FullSyncPipeline) spec() PipelineSpec { return pipeline.PipelineSpec }
func (pipeline *FullSyncPipeline) isFullSync() bool   { return true }
func (pipeline *FullSyncPipeline) sync(job *job, ctx context.Context) error {
	runner := job.runner

	syncJobState := &SyncJobState{}
	err := runner.store.GetObject(server.JOB_DATA_INDEX, job.id, syncJobState)
	if err != nil {
		return err
	}
	syncJobState.ID = job.id // add the id to the sync state

	keepReading := true

	//dont call source.startFullSync, just sink.startFullSync. to make sure we run on changes.
	//exception is when the sink is http.
	if pipeline.sink.GetConfig()["Type"] == "HttpDatasetSink" {
		pipeline.source.startFullSync()
	}
	err = pipeline.sink.startFullSync(runner)
	if err != nil {
		return err
	}
	syncJobState.ContinuationToken = ""

	for keepReading {

		err := pipeline.source.readEntities(runner, syncJobState.ContinuationToken, pipeline.batchSize, func(entities []*server.Entity, continuationToken string) error {
			select {
			// if the cancellable context is cancelled, ctx.Done will trigger, and it will break out. The only way I
			// found to do so, was to trigger an error, and then check for that in the jobs.Runner.
			case <-ctx.Done():
				keepReading = false
				return errors.New("got job interrupt")
			default:
				var err error
				incomingEntityCount := len(entities)

				if incomingEntityCount > 0 {
					// apply transform if it exists
					if pipeline.transform != nil {
						entities, err = pipeline.transform.transformEntities(runner, entities)
						if err != nil {
							return err
						}
					}

					// write to sink
					err = pipeline.sink.processEntities(runner, entities)
					if err != nil {
						return err
					}
				}

				// capture token if there is one
				if continuationToken != "" {
					syncJobState.ContinuationToken = continuationToken

				}

				if incomingEntityCount == 0 || // if this was the last page (empty) of a tokenized source
					continuationToken == "" { // OR it was not a tokenized  source
					keepReading = false // then stop reading, we are done
				}
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	pipeline.source.endFullSync()
	err = pipeline.sink.endFullSync(runner)
	if err != nil {
		return err
	}

	//Do not store syncState when the target is an http sink.
	//Since we use entities for httpsinks, the continuation tokens are base64 strings and not compatible with incremental tokens
	if pipeline.sink.GetConfig()["Type"] != "HttpDatasetSink" {
		err = runner.store.StoreObject(server.JOB_DATA_INDEX, job.id, syncJobState)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pipeline *IncrementalPipeline) spec() PipelineSpec { return pipeline.PipelineSpec }
func (pipeline *IncrementalPipeline) isFullSync() bool   { return false }
func (pipeline *IncrementalPipeline) sync(job *job, ctx context.Context) error {
	runner := job.runner

	syncJobState := &SyncJobState{}
	err := runner.store.GetObject(server.JOB_DATA_INDEX, job.id, syncJobState)
	if err != nil {
		return err
	}

	syncJobState.ID = job.id // add the id to the sync state

	keepReading := true

	for keepReading {

		err := pipeline.source.readEntities(runner, syncJobState.ContinuationToken, pipeline.batchSize, func(entities []*server.Entity, continuationToken string) error {
			select {
			// if the cancellable context is cancelled, ctx.Done will trigger, and it will break out. The only way I
			// found to do so, was to trigger an error, and then check for that in the jobs.Runner.
			case <-ctx.Done():
				keepReading = false
				return errors.New("got job interrupt")
			default:
				var err error
				incomingEntityCount := len(entities)

				if incomingEntityCount > 0 {
					// apply transform if it exists
					if pipeline.transform != nil {
						entities, err = pipeline.transform.transformEntities(runner, entities)
						if err != nil {
							return err
						}
					}

					// write to sink
					err = pipeline.sink.processEntities(runner, entities)
					if err != nil {
						return err
					}
				}

				// store token if there is one
				if continuationToken != "" {
					syncJobState.ContinuationToken = continuationToken

					err = runner.store.StoreObject(server.JOB_DATA_INDEX, job.id, syncJobState)
					if err != nil {
						return err
					}
				}

				if incomingEntityCount == 0 || // if this was the last page (empty) of a tokenized source
					continuationToken == "" { // OR it was not a tokenized  source
					keepReading = false // then stop reading, we are done
				}
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}
