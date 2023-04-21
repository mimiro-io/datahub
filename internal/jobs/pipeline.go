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
	"math"
	"reflect"
	"sync"
	"time"

	jobSource "github.com/mimiro-io/datahub/internal/jobs/source"

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
	source    jobSource.Source
	sink      Sink
	transform Transform
	batchSize int
}
type (
	FullSyncPipeline    struct{ PipelineSpec }
	IncrementalPipeline struct{ PipelineSpec }
)

func (pipeline *FullSyncPipeline) spec() PipelineSpec { return pipeline.PipelineSpec }
func (pipeline *FullSyncPipeline) isFullSync() bool   { return true }
func (pipeline *FullSyncPipeline) sync(job *job, ctx context.Context) error {
	runner := job.runner

	syncJobState := &SyncJobState{}
	err := runner.store.GetObject(server.JobDataIndex, job.id, syncJobState)
	if err != nil {
		return err
	}
	syncJobState.ID = job.id // add the id to the sync state

	keepReading := true

	// dont call source.startFullSync, just sink.startFullSync. to make sure we run on changes.
	// exception is when the sink is http(we want to process entities instead of changes)
	// or source is multisource (we need to grab watermarks at beginning of fullsync)
	if pipeline.sink.GetConfig()["Type"] == "HttpDatasetSink" ||
		pipeline.source.GetConfig()["Type"] == "MultiSource" {
		// if pipeline.source.GetConfig()["Type"] == "MultiSource" {
		// 	return errors.New("MultiSource can only produce changes and must therefore not be used with HttpDatasetSink")
		// }
		pipeline.source.StartFullSync()
	}
	err = pipeline.sink.startFullSync(runner)
	if err != nil {
		return err
	}
	syncJobState.ContinuationToken = ""

	tags := []string{"application:datahub", "job:" + job.title}
	for keepReading {

		processEntities := func(entities []*server.Entity, continuationToken jobSource.DatasetContinuation) error {
			select {
			// if the cancellable context is cancelled, ctx.Done will trigger, and it will break out. The only way I
			// found to do so, was to trigger an error, and then check for that in the jobs.Runner.
			case <-ctx.Done():
				keepReading = false
				return errors.New("got job interrupt")
			default:
				var err2 error
				incomingEntityCount := len(entities)

				if incomingEntityCount > 0 {
					// apply transform if it exists
					if pipeline.transform != nil {
						transformTS := time.Now()
						entities, err2 = pipeline.transform.transformEntities(runner, entities, job.title)
						_ = runner.statsdClient.Timing("pipeline.transform.batch", time.Since(transformTS), tags, 1)
						if err2 != nil {
							return err2
						}
					}

					// write to sink
					sinkTS := time.Now()
					err2 = pipeline.sink.processEntities(runner, entities)
					_ = runner.statsdClient.Timing("pipeline.sink.batch", time.Since(sinkTS), tags, 1)
					if err2 != nil {
						return err2
					}
				}

				// capture token if there is one
				if continuationToken.GetToken() != "" {
					syncJobState.ContinuationToken, err2 = continuationToken.Encode()
					if err2 != nil {
						return err2
					}
				}

				if incomingEntityCount == 0 || // if this was the last page (empty) of a tokenized source
					continuationToken.GetToken() == "" { // OR it was not a tokenized  source
					keepReading = false // then stop reading, we are done
				}
			}
			return nil
		}

		readTS := time.Now()
		token, err2 := jobSource.DecodeToken(pipeline.source.GetConfig()["Type"], syncJobState.ContinuationToken)
		if err2 != nil {
			return err2
		}

		err = pipeline.source.ReadEntities(token, pipeline.batchSize,
			func(entities []*server.Entity, c jobSource.DatasetContinuation) error {
				_ = runner.statsdClient.Timing("pipeline.source.batch", time.Since(readTS), tags, 1)
				result := processEntities(entities, c)
				readTS = time.Now()
				return result
			})

		if err != nil {
			return err
		}
	}

	pipeline.source.EndFullSync()
	err = pipeline.sink.endFullSync(runner)
	if err != nil {
		return err
	}

	//Do not store syncState when the target is an http sink.
	//Since we use entities for httpsinks, the continuation tokens are base64 strings and not compatible with incremental tokens
	//
	//Exception is when used with MultiSource... MultiSource only operates on changes. also in fullsync mode.
	//   Difference there between fullsync and incremental is whether dependencies are processed
	if pipeline.sink.GetConfig()["Type"] != "HttpDatasetSink" || pipeline.source.GetConfig()["Type"] == "MultiSource" {
		err = runner.store.StoreObject(server.JobDataIndex, job.id, syncJobState)
		if err != nil {
			return err
		}
	}

	return nil
}

type presult struct {
	entities []*server.Entity
	err      error
}

func (pipeline *IncrementalPipeline) spec() PipelineSpec { return pipeline.PipelineSpec }
func (pipeline *IncrementalPipeline) isFullSync() bool   { return false }
func (pipeline *IncrementalPipeline) sync(job *job, ctx context.Context) error {
	runner := job.runner

	syncJobState := &SyncJobState{}
	err := runner.store.GetObject(server.JobDataIndex, job.id, syncJobState)
	if err != nil {
		return err
	}

	syncJobState.ID = job.id // add the id to the sync state

	keepReading := true

	tags := []string{"application:datahub", "job:" + job.title}
	for keepReading {

		processEntities := func(entities []*server.Entity, continuationToken jobSource.DatasetContinuation) error {
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
						transformTS := time.Now()

						parallelisms := pipeline.transform.getParallelism()
						if len(entities) < parallelisms {
							parallelisms = 1
						}

						psize := int(math.Round(float64(len(entities)) / float64(parallelisms)))
						workResults := make([]presult, parallelisms)

						local := func(workId int, lentities []*server.Entity, wg *sync.WaitGroup) {
							res := presult{}
							if reflect.TypeOf(pipeline.transform) == reflect.TypeOf(&JavascriptTransform{}) {
								t := pipeline.transform.(*JavascriptTransform)
								tc, _ := t.Clone()
								pe, e := tc.transformEntities(runner, lentities, job.title)
								res.entities = pe
								res.err = e
							} else {
								pe, e := pipeline.transform.transformEntities(runner, lentities, job.title)
								res.entities = pe
								res.err = e
							}
							workResults[workId] = res
							wg.Done()
						}

						var wg sync.WaitGroup
						wg.Add(parallelisms)

						wid := 0
						index := 0
						for i := 0; i < parallelisms; i++ {
							from := index
							to := index + psize

							if to >= len(entities) {
								to = index + (len(entities) - index)
							}

							chunk := make([]*server.Entity, to-from)
							copy(chunk, entities[from:to])
							go local(wid, chunk, &wg)
							wid++
							index += psize
						}

						wg.Wait()

						// construct result
						entities = make([]*server.Entity, 0)
						for i := 0; i < parallelisms; i++ {
							res := workResults[i]
							if res.err != nil {
								return res.err
							}
							entities = append(entities, res.entities...)
						}

						err = runner.statsdClient.Timing("pipeline.transform.batch", time.Since(transformTS), tags, 1)
						if err != nil {
							return err
						}
					}

					// write to sink
					sinkTS := time.Now()
					err = pipeline.sink.processEntities(runner, entities)
					_ = runner.statsdClient.Timing("pipeline.sink.batch", time.Since(sinkTS), tags, 1)
					if err != nil {
						return err
					}
				}

				// store token if there is one
				if continuationToken.GetToken() != "" {
					syncJobState.ContinuationToken, err = continuationToken.Encode()
					if err != nil {
						return err
					}

					err = runner.store.StoreObject(server.JobDataIndex, job.id, syncJobState)
					if err != nil {
						return err
					}
				}

				if incomingEntityCount == 0 || // if this was the last page (empty) of a tokenized source
					continuationToken.GetToken() == "" { // OR it was not a tokenized  source
					keepReading = false // then stop reading, we are done
				}
			}
			return nil
		}

		readTS := time.Now()
		since, err := jobSource.DecodeToken(pipeline.source.GetConfig()["Type"], syncJobState.ContinuationToken)
		if err != nil {
			return err
		}
		err = pipeline.source.ReadEntities(since,
			pipeline.batchSize,
			func(entities []*server.Entity, c jobSource.DatasetContinuation) error {
				_ = runner.statsdClient.Timing("pipeline.source.batch", time.Since(readTS), tags, 1)
				result := processEntities(entities, c)
				readTS = time.Now()
				return result
			})

		if err != nil {
			return err
		}
	}

	return nil
}
