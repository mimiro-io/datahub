package jobs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

type (
	// tableInput is a struct that contains the input to a test
	tableInput struct {
		onErrorJSON string
		source      source.Source
		sink        Sink
		transform   Transform
	}

	// expected is a struct that contains the expected results of a test
	expected struct {
		ErrorHandlers []*ErrorHandler           // error handlers expected to be set on the job based on config input
		InitError     error                     // error expected from config validation
		SyncError     error                     // error expected from pipeline.Sync()
		Runs          int                       // number of times the source (and therefore the whole pipeline) was run
		Logged        func(batchSize int) []int // function generating list of ids of entities that were logged
		ReQueued      []int                     // ids of entities that were requeued
		Processed     func(batchSize int) int   // function calculating expected numbor of processed (pulled from source) entities
		SinkRecorded  func(batchSize int) []int // function providing expected entities written to sink
	}

	////////////////////////// the following types are mock types for testing //////////////////////////

	failingSource struct {
		source.SampleSource
	}

	failingSink struct {
		devNullSink
		recoverAfterRun int
		count           int
		maxBatchSize    int
		succeed         bool
	}

	recordingSink struct {
		recorded []int
	}

	pickySink struct {
		recordingSink
	}

	recordingLogHandler struct {
		f        failingEntityHandler
		recorded []int
	}

	countingPipeline struct {
		count   int
		runChan chan bool
		p       Pipeline
	}
)

func (c *countingPipeline) sync(job *job, ctx context.Context) (int, error) {
	if c.runChan == nil {
		c.runChan = make(chan bool, 100)
	}
	c.runChan <- true
	c.count++
	wrapped, isWrapped := c.p.spec().sink.(*wrappedSink)
	if isWrapped && wrapped.s != nil {
		if s, isFailingSink := wrapped.s.(*failingSink); isFailingSink {
			if s.recoverAfterRun > 0 && c.count > s.recoverAfterRun {
				s.succeed = true
			}
		}
	}
	return c.p.sync(job, ctx)
}

func (c *countingPipeline) spec() *PipelineSpec { return c.p.spec() }
func (c *countingPipeline) isFullSync() bool    { return c.p.isFullSync() }

var _ = Describe("A failed trigger", func() {
	testCnt := 0
	var dsm *server.DsManager
	var scheduler *Scheduler
	var store *server.Store
	var runner *Runner
	var storeLocation string
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./error_handler_test_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		scheduler, store, runner, dsm,
			_ = setupScheduler(storeLocation)
	})
	AfterEach(func() {
		runner.Stop()
		//_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	DescribeTable("with",

		// This function is the evaluation function, called for every Entry in the table
		func(input tableInput, behaviour expected) {
			// first, we set up the job config
			errorHandlers := ""

			// using random batch size to make sure we cover issues where MaxItems in error handlers don't match batch size
			rndBatchSize := rand.Intn(10) + 1
			//rndBatchSize = 10
			tag := fmt.Sprintf("(input was %+v, batchSize:%v)", input, rndBatchSize)
			if input.onErrorJSON != "" {
				errorHandlers = fmt.Sprintf(`,"onError": %v`, input.onErrorJSON)
			}
			jobConfig := fmt.Sprintf(`{
				"id": "test",
				"title": "test",
				"batchSize": %v,
				"triggers": [{
				"triggerType": "cron",
				"jobType": "incremental",
				"schedule": "@every 2s"%v
				}],
				"paused": true,
				"source" : { "Type" : "SampleSource", "NumberOfEntities": 10 },
				"sink" : { "Type" : "DevNullSink" }
				}`, rndBatchSize, errorHandlers)

			// println(jobConfig)

			// 2nd, we json parse the job config
			j, err := scheduler.Parse([]byte((jobConfig)))
			if err != nil {
				Expect(err).To(BeEquivalentTo(behaviour.InitError), tag)
				return
			}

			// 3rd we add the job to the scheduler (implicitly calling verifyErrorHandlers)
			err = scheduler.AddJob(j)
			if err != nil {
				Expect(err).To(BeEquivalentTo(behaviour.InitError), tag)
				return
			}
			// fixup: normally, the retryDelay is seconds, for our tests we want it to be ns
			for _, eh := range j.Triggers[0].ErrorHandlers {
				eh.RetryDelay = eh.RetryDelay / int64(time.Second)
			}

			if behaviour.InitError != nil {
				Fail(fmt.Sprintf("Error did not occur as expected: %v", behaviour.InitError))
			}

			// At this point, we have a job with error handlers, and we can verify that they are set correctly
			Expect(j.Triggers).To(HaveLen(1), tag)

			// nil out internal fields for comparison, and swap in log recorder
			comparableErrorHandlers := make([]*ErrorHandler, len(j.Triggers[0].ErrorHandlers))
			recorder := &recordingLogHandler{}
			if j.Triggers[0].ErrorHandlers != nil {
				for i, eh := range j.Triggers[0].ErrorHandlers {
					ehCopy := *eh
					ehCopy.failingEntityHandler = nil
					comparableErrorHandlers[i] = &ehCopy

					if eh.Type == "log" {
						recorder.f = eh.failingEntityHandler
						eh.failingEntityHandler = recorder
					}
				}
			}
			Expect(comparableErrorHandlers).To(BeEquivalentTo(behaviour.ErrorHandlers), tag)

			// 4th we prepare a pipeline with overrides if given (redoing parts of what AddJob did in 3rd)
			jobs, err := scheduler.toTriggeredJobs(j)
			Expect(err).To(BeNil(), tag)
			Expect(jobs).To(HaveLen(1), tag)

			if input.source != nil {
				jobs[0].pipeline.(*IncrementalPipeline).source = input.source
			}
			if input.sink != nil {
				jobs[0].pipeline.(*IncrementalPipeline).sink = input.sink
			}
			if input.transform != nil {
				jobs[0].pipeline.(*IncrementalPipeline).transform = input.transform
			}

			cntP := &countingPipeline{p: jobs[0].pipeline}
			jobs[0].pipeline = cntP

			// take a note of number of existing datasets before run
			existingDatasetNames := dsm.GetDatasetNames()

			// 5th we run the pipeline
			jobs[0].Run()

			if len(behaviour.ReQueued) > 0 {
				// if we expect requeued items, check that there is a requeue dataset
				Expect(len(dsm.GetDatasetNames())).To(Equal(len(existingDatasetNames) + 1))
			}
			// verify number of pipeline runs
			if behaviour.Runs != 0 {
				cnt := 0
				func() {
					for {
						select {
						/* purpose of the channel here:
						the countingPipeline puts a signal on the channel when it is called, and we count here.
						The fallthrough channel select resets the timeout every time a signal is received.
						So unlimited number of retries can be tested, and each retry has 100ms to call the pipeline.

						This also gives us a trailing timeout which can detect when more retries than expected
							are happening.

						This solution is still timing dependent, so not 100% robust.
						*/
						case <-cntP.runChan:
							cnt += 1
						case <-time.After(100 * time.Millisecond):
							if cntP.runChan != nil {
								// close(cntP.runChan)
							}
							return
						}
					}
				}()
				Expect(cnt).To(Equal(behaviour.Runs), tag)
			}

			// after the run and hopefully all reruns, fetch job result from badger
			r := jobResult{}
			err = store.GetObject(server.JobResultIndex, jobs[0].id, &r)
			Expect(err).To(BeNil(), tag)
			err = nil
			if r.LastError != "" {
				err = errors.New(r.LastError)
			}
			processed := r.Processed

			// verify expected error state after job run
			if behaviour.SyncError != nil {
				Expect(err).To(BeEquivalentTo(behaviour.SyncError), tag)
			} else {
				Expect(err).To(BeNil(), tag)
			}

			// verify number of entities pulled from source
			if behaviour.Processed != nil {
				expectedProcessed := behaviour.Processed(rndBatchSize)
				if expectedProcessed != -1 {
					Expect(processed).To(Equal(expectedProcessed), tag)
				}
			} else {
				// default sampleSource produces 10 entities
				Expect(processed).To(Equal(10), tag)
			}

			// verify expected list of logged entities
			if behaviour.Logged != nil {
				Expect(recorder.recorded).To(BeEquivalentTo(behaviour.Logged(rndBatchSize)), tag)
			}

			if behaviour.SinkRecorded != nil {
				exp := behaviour.SinkRecorded(rndBatchSize)
				wrapped, isWrapped := jobs[0].pipeline.spec().sink.(*wrappedSink)
				if isWrapped {
					recordedSink, ok := wrapped.s.(*recordingSink)
					if !ok {
						pickySink, ok := wrapped.s.(*pickySink)
						if ok {
							recordedSink = &pickySink.recordingSink
						}
					}
					Expect(recordedSink).NotTo(BeNil(), tag)
					Expect(recordedSink.recorded).To(BeEquivalentTo(exp), tag)
				} else {
					// this test case had no error handlers, therefore no wrappedSink
					// but maybe it is a picky sink
					pickySink, ok := jobs[0].pipeline.spec().sink.(*pickySink)
					if ok {
						if len(exp) == 0 {
							Expect(pickySink.recorded).To(HaveLen(0), tag)
						} else {
							Expect(pickySink.recorded).To(BeEquivalentTo(exp), tag)
						}
					}
				}
			}

			if behaviour.ReQueued != nil {
				jobs[0].pipeline.spec().source = &source.SampleSource{
					NumberOfEntities: 0,
					BatchSize:        1,
					Store:            store,
				}
				// attach new recordingSink to register emitted entities
				reQueueSink := &recordingSink{}
				jobs[0].pipeline.spec().sink = reQueueSink
				// also nil out transform to make sure the run succeeds
				jobs[0].pipeline.spec().transform = nil
				jobs[0].Run()

				// verify all requeued items are emitted
				Expect(reQueueSink.recorded).To(BeEquivalentTo(behaviour.ReQueued), tag)

				// verify thar no requeue dataset remains in store
				Expect(len(dsm.GetDatasetNames())).To(Equal(len(existingDatasetNames)), tag)
			}
		},

		// This function generates the description of the Entry if not provided explicitly
		func(input tableInput, behaviour expected) string {
			return fmt.Sprintf("should accept %v and do %v ", input, behaviour)
		},

		////////////////////////// First bunch of Entries tests configuration validation //////////////////////////
		Entry("no handlers",
			tableInput{onErrorJSON: ""},
			expected{
				ErrorHandlers: []*ErrorHandler{},
				InitError:     nil,
			}),
		Entry("empty handlers",
			tableInput{onErrorJSON: "[]"}, expected{
				ErrorHandlers: []*ErrorHandler{},
				InitError:     nil,
			}),
		Entry("empty handler object",
			tableInput{onErrorJSON: "[{}]"}, expected{
				ErrorHandlers: nil,
				InitError:     errors.New("need to set 'errorHandler'. must be one of: reRun, reQueue, log"),
			}),
		Entry("missing errorHandler in handler object",
			tableInput{onErrorJSON: `[{"maxItems": 5}]`}, expected{
				ErrorHandlers: nil,
				InitError:     errors.New("need to set 'errorHandler'. must be one of: reRun, reQueue, log"),
			}),
		Entry(
			"unknown errorHandler in handler object",
			tableInput{onErrorJSON: `[{"errorHandler": "bogus"}]`},
			expected{
				ErrorHandlers: nil,
				InitError:     errors.New("need to set 'errorHandler'. must be one of: reRun, reQueue, log"),
			},
		),
		Entry(
			"one invalid handler should fail the whole list ",
			tableInput{onErrorJSON: `[{"errorHandler": "log"}, {"errorHandler": "bogus"}]`},
			expected{
				ErrorHandlers: nil,
				InitError:     errors.New("need to set 'errorHandler'. must be one of: reRun, reQueue, log"),
			},
		),
		Entry("one valid handler should be accepted", tableInput{onErrorJSON: `[{"errorHandler": "log"}]`}, expected{
			ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 0}},
			InitError:     nil,
		}),
		Entry("capital spelling should be accepted, and default values should be set",
			tableInput{onErrorJSON: `[{"errorHandler": "Log"},{"errorHandler": "ReRun"},{"errorHandler": "ReQueUe"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{
					{Type: "log", MaxItems: 0}, // 0 is the default. meaning unlimited
					{Type: "rerun", RetryDelay: 30, MaxRetries: 1},
					{Type: "requeue", MaxItems: 0},
				},
				InitError: nil,
			},
		),
		Entry(
			"Log handler should accept maxItems",
			tableInput{onErrorJSON: `[{"errorHandler": "log", "maxItems": 5}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 5}},
				InitError:     nil,
			},
		),
		Entry(
			"requeue handler should accept maxItems",
			tableInput{onErrorJSON: `[{"errorHandler": "reQueue", "maxItems": 15}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 15}},
				InitError:     nil,
			},
		),
		Entry(
			"rerun handler should accept maxRetries and retryDelay",
			tableInput{onErrorJSON: `[{"errorHandler": "reRun", "maxRetries": 7, "retryDelay": 30}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "rerun", MaxRetries: 7, RetryDelay: 30}},
				InitError:     nil,
				Runs:          1, // reRun configured, but no failures, so job should run only once
			},
		),
		Entry(
			"duplicate handlers should be rejected",
			tableInput{onErrorJSON: `[{"errorHandler": "log"}, {"errorHandler": "rerun"}, {"errorHandler": "Log"}]`},
			expected{
				ErrorHandlers: nil,
				InitError:     errors.New("duplicate error handler: log"),
			},
		),

		////////////////////////// Test the 3 error handler types (and combinations) with failing source.  /////////////
		Entry(
			"failing source with no handlers should just fail the pipeline",
			tableInput{source: &failingSource{}},
			expected{
				ErrorHandlers: []*ErrorHandler{},
				SyncError:     errors.New("failing source"),
				Processed:     func(batchSize int) int { return 0 },
			},
		),
		Entry(
			"failing source with log handler should just fail the pipeline",
			tableInput{source: &failingSource{}, onErrorJSON: `[{"errorHandler": "log"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 0}},
				SyncError:     errors.New("failing source"), // expect job status to be failed
				Processed:     func(batchSize int) int { return 0 },
			},
		),
		Entry(
			"failing source with reQueue handler should just fail the pipeline",
			tableInput{source: &failingSource{}, onErrorJSON: `[{"errorHandler": "requeue"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 0}},
				SyncError:     errors.New("failing source"),
				Processed:     func(batchSize int) int { return 0 },
			},
		),
		Entry(
			"failing source with reRun handler should just fail the pipeline n times",
			tableInput{
				source:      &failingSource{},
				onErrorJSON: `[{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 3}]`,
			},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "rerun", MaxRetries: 3, RetryDelay: 1}}, // 5 ns delay
				SyncError:     errors.New("failing source"),
				Processed:     func(batchSize int) int { return 0 },
				Runs:          4, // check the pipeline was run 4 time (one regular + 3 retries)
			},
		),
		Entry(
			"failing source with all handlers should just fail the pipeline n times",
			tableInput{
				source: &failingSource{},
				onErrorJSON: `[
					{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 3},
					{"errorHandler": "log"},
					{"errorHandler": "reQueue"}
				]`,
			},
			expected{
				ErrorHandlers: []*ErrorHandler{
					{Type: "rerun", MaxRetries: 3, RetryDelay: 1}, // 1 ns delay
					{Type: "log"},
					{Type: "requeue"},
				},
				SyncError: errors.New("failing source"),
				Processed: func(batchSize int) int { return 0 },
				Runs:      4, // check the pipeline was run 4 time (one regular + 3 retries)
			},
		),

		////////////////////////// Test the 3 error handler types (and combinations) with failing sink.  /////////////
		Entry("failing sink with no handlers should just fail the pipeline",
			tableInput{sink: &failingSink{}},
			expected{
				ErrorHandlers: []*ErrorHandler{},
				SyncError:     errors.New("failing sink"),
				Processed:     func(batchSize int) int { return int(math.Min(float64(batchSize), 10.0)) },
			},
		),
		Entry("failing sink with reRun handler should repeat run n times",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 3}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "rerun", MaxRetries: 3, RetryDelay: 1}}, // 5 ns delay
				SyncError:     errors.New("failing sink"),
				Runs:          4,
				Processed:     func(batchSize int) int { return int(math.Min(float64(batchSize), 10.0)) },
			},
		),
		Entry("failing sink with unlimited log handler should log all entities",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "log"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 0}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				Logged:        func(batchSizei int) []int { return []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} },
			},
		),
		Entry("failing sink with capped log handler should log maxItems entities",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "log", "maxItems": 5}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 5}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				Processed: func(batchSize int) int {
					if batchSize > 5 {
						return int(math.Min(float64(batchSize), 10.0))
					}
					return map[int]int{1: 6, 2: 6, 3: 6, 4: 8, 5: 10}[batchSize]
				},
				Logged: func(batchSize int) []int { return []int{0, 1, 2, 3, 4} },
			},
		),
		Entry("failing sink with log handler maxItems=1 should log first entity",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "log", "maxItems": 1}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 1}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				Logged:        func(batchSize int) []int { return []int{0} },
				Processed:     func(batchSize int) int { return int(math.Min(math.Max(float64(2), float64(batchSize)), 10)) },
			},
		),
		Entry("failing sink with log handler where cap = batch size should log maxItems entities",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "log", "maxItems": 10}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 10}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				Logged:        func(batchSize int) []int { return []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} },
			},
		),
		Entry("failing sink with log handler where cap > batch size should log like unlimited log handler",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "log", "maxItems": 11}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 11}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				Logged:        func(batchSize int) []int { return []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} },
			},
		),

		Entry("failing sink with log handler and reRun handler",
			tableInput{
				sink:        &failingSink{},
				onErrorJSON: `[{"errorHandler": "log"},{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 2}]`,
			},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log"}, {Type: "rerun", MaxRetries: 2, RetryDelay: 1}},
				SyncError:     errors.New("failing sink"),
				Runs:          3,
				Logged:        func(batchSize int) []int { return []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} },
			},
		),
		Entry("failing sink with capped log handler and reRun handler",
			tableInput{
				sink:        &failingSink{},
				onErrorJSON: `[{"errorHandler": "log", "maxItems": 5},{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 2}]`,
			},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 5}, {Type: "rerun", MaxRetries: 2, RetryDelay: 1}},
				SyncError:     errors.New("failing sink"),
				Runs:          3,
				Logged: func(batchSize int) []int {
					if batchSize < 6 {
						// depending on the batch size, when the last batch is only partially logged, it must be
						// reprocessed during a rerun. this will give duplicate logging of the overlapping entities.
						// TODO: should we consider constructing partial continuation tokens here? so that the retry
						// 	picks up after the last logged entity?
						return map[int][]int{
							1: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
							2: {0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 8, 9},
							3: {0, 1, 2, 3, 4, 3, 4, 5, 6, 7, 6, 7, 8, 9},
							4: {0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 8, 9},
							5: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
						}[batchSize]
					}
					return []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4}
				},
				Processed: func(batchSize int) int {
					if batchSize > 5 {
						return int(math.Min(float64(batchSize), 10.0))
					}
					return map[int]int{1: 6, 2: 6, 3: 6, 4: 8, 5: 10}[batchSize]
				},
			},
		),
		Entry("failing sink with reQueue handler should requeue all entities",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "requeue"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue"}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				ReQueued:      []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			}),
		Entry("failing sink with capped reQueue handler should requeue up to cap and then fail",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "requeue", "maxItems": 3}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 3}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				// requeue should contain our 3 allowed items.
				// note there may be overlap on rerun with current cont-token, but this is not tested for here
				ReQueued:  []int{0, 1, 2},
				Processed: func(batchSize int) int { return int(math.Min(float64((3/batchSize+1)*batchSize), 10)) },
			}),
		Entry("failing sink with capped reQueue handler and uncapped log handler",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "requeue", "maxItems": 3},{"errorHandler": "log"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 3}, {Type: "log"}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				// requeue should contain our 3 allowed items.
				ReQueued: []int{0, 1, 2},
				// strictest handler dictates how far we go through the source. requeue mad log stop at 3
				Logged:    func(batchSize int) []int { return []int{0, 1, 2} },
				Processed: func(batchSize int) int { return int(math.Min(float64((3/batchSize+1)*batchSize), 10)) },
			}),
		Entry("failing sink with uncapped reQueue handler and capped log handler",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "requeue"},{"errorHandler": "log", "maxItems": 3}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue"}, {Type: "log", MaxItems: 3}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				// due to handler order, requeue manages to pick up one more before log stops pipeline
				ReQueued: []int{0, 1, 2, 3},
				// strictest handler dictates how far we go through the source.
				Logged:    func(batchSize int) []int { return []int{0, 1, 2} },
				Processed: func(batchSize int) int { return int(math.Min(float64((3/batchSize+1)*batchSize), 10)) },
			}),

		Entry("failing sink with uncapped reQueue handler and reRun",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "requeue"},{"errorHandler": "reRun", "retryDelay":1,"maxRetries": 3}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue"}, {Type: "rerun", RetryDelay: 1, MaxRetries: 3}},
				SyncError:     errors.New("failing sink"),
				Runs:          4,
				ReQueued:      []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
				Processed:     func(batchSize int) int { return 10 },
			}),

		// TODO: reboot test. make a sink that simulates restart of datahub in middle of processing (reparses job etc and does a new run. make sure no reQueue item is lost)
		// TODO: how to deal with latestOnly? when the requeued item is not latest anymore at time of reprocessing

		// recovering means the sink fails the first job run, but succeeds on the second attempt
		// we want to make sure that we still log the failed entities the first time around,
		// but also end up in a succeeded state after the second attempt
		Entry("recovering sink with capped log handler and reRun handler",
			tableInput{
				sink:        &failingSink{recoverAfterRun: 1},
				onErrorJSON: `[{"errorHandler": "log", "maxItems": 2},{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 5}]`,
			},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 2}, {Type: "rerun", MaxRetries: 5, RetryDelay: 1}},
				Runs:          2, // not maxing out reRuns, because sink recovers
				Logged: func(batchSize int) []int {
					return []int{0, 1} // logging 2 entities during first run
				},
				Processed: func(batchSize int) int {
					// for batch size up to 2, the first batch is completely handled
					// by log handler, so it does not come up again during rerun
					// for larger batches, the run fails the first batch after logging 2 entities,
					// so the first batch is reprocessed during rerun
					if batchSize < 3 {
						return 8
					} else {
						return 10
					}
				},
			}),

		// single entities in sink fail
		// picky sink has some accepted entities and some failing entities
		Entry("picky sink without error handlers",
			tableInput{sink: &pickySink{}},
			expected{
				ErrorHandlers: []*ErrorHandler{},
				SyncError:     errors.New("picky sink"),
				Runs:          1,
				Processed: func(batchSize int) int {
					if batchSize <= 3 {
						return (3/batchSize)*batchSize + batchSize
					}
					return int(math.Min(float64(batchSize), 10))
				},
				SinkRecorded: func(batchSize int) []int {
					if batchSize == 1 || batchSize == 3 {
						return []int{0, 1, 2}
					}
					if batchSize == 2 {
						return []int{0, 1}
					}
					return []int{}
				},
			},
		),
		Entry("picky sink log handler",
			tableInput{sink: &pickySink{}, onErrorJSON: `[{"errorHandler": "log"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log"}},
				SyncError:     errors.New("picky sink"),
				Runs:          1,
				//uncapped log handler, so expecting to go through complete source
				Processed: func(batchSize int) int { return 10 },
				Logged: func(batchSize int) []int {
					return []int{3, 6, 9}
				},
				SinkRecorded: func(batchSize int) []int { return []int{0, 1, 2, 4, 5, 7, 8} },
			},
		),
		Entry("picky sink with reRun handler",
			tableInput{sink: &pickySink{}, onErrorJSON: `[{"errorHandler": "reRun", "retryDelay": 1, "maxRetries": 3}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "rerun", MaxRetries: 3, RetryDelay: 1}},
				SyncError:     errors.New("picky sink"),
				Runs:          4,
				//uncapped log handler, so expecting to go through complete source
				Processed: func(batchSize int) int { return int(math.Min(float64(batchSize), 10)) },
				SinkRecorded: func(batchSize int) []int {
					if batchSize == 1 || batchSize == 3 {
						return []int{0, 1, 2}
					}
					if batchSize == 2 {
						return []int{0, 1}
					}
					return []int{}
				},
			},
		),
		Entry("picky sink with capped reQueue handler",
			tableInput{sink: &pickySink{}, onErrorJSON: `[{"errorHandler": "reQueue", "maxItems": 1}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 1}},
				SyncError:     errors.New("picky sink"),
				Runs:          1,
				ReQueued:      []int{3},
				Processed: func(batchSize int) int {
					if batchSize < 10 {
						if batchSize <= 6 {
							x := 6/batchSize*batchSize + batchSize
							return int(math.Min(float64(x), 10))
						}
						return batchSize
					}
					return 10
				},
				SinkRecorded: func(batchSize int) []int { return []int{0, 1, 2, 4, 5} },
			},
		),

		////////////////////////// Test the 3 error handler types (and combinations) with failing transform.  /////////////
		// failing tranfsorm: all handlers applied
		// bug in transform: all entities fail
		// single entities in transform fail

		// cancelled job should not retry, how to test?
	)
})

func (fs *failingSink) processEntities(runner *Runner, entities []*server.Entity) error {
	if fs.succeed {
		return fs.devNullSink.processEntities(runner, entities)
	}
	return errors.New("failing sink")
}

func (fs *failingSource) ReadEntities(cont source.DatasetContinuation, limit int, f func(entities []*server.Entity, continuation source.DatasetContinuation) error) error {
	return errors.New("failing source")
}

func (r *recordingLogHandler) reset() {
	r.f.reset()
}

func (r *recordingLogHandler) handleFailingEntity(runner *Runner, entity *server.Entity, jobId string) error {
	result := r.f.handleFailingEntity(runner, entity, jobId)
	if result != nil {
		return result
	}
	recId, err := strconv.Atoi(strings.Split(entity.ID, "-")[1])
	if err != nil {
		return err
	}
	r.recorded = append(r.recorded, recId)
	return nil
}

func (p *pickySink) processEntities(runner *Runner, entities []*server.Entity) error {
	var batchRecord []int
	for _, entity := range entities {
		tokens := strings.Split(entity.ID, "-")
		id, err := strconv.Atoi(tokens[1])
		if err != nil {
			return err
		}
		mod := id % 3
		if id > 0 && mod == 0 {
			return errors.New("picky sink")
		}
		batchRecord = append(batchRecord, id)
	}
	p.recorded = append(p.recorded, batchRecord...)
	return nil
}

func (p *recordingSink) processEntities(runner *Runner, entities []*server.Entity) error {
	var batchRecord []int
	for _, entity := range entities {
		id, err := strconv.Atoi(strings.Split(entity.ID, "-")[1])
		if err != nil {
			return err
		}
		batchRecord = append(batchRecord, id)
	}
	p.recorded = append(p.recorded, batchRecord...)
	return nil
}

func (p *recordingSink) GetConfig() map[string]interface{} {
	return map[string]interface{}{"Type": "test"}
}
func (p *recordingSink) startFullSync(runner *Runner) error { panic("implement me") }
func (p *recordingSink) endFullSync(runner *Runner) error   { panic("implement me") }