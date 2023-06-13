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
		source      testSource
		sink        Sink
		transform   Transform
	}

	// expected is a struct that contains the expected results of a test
	expected struct {
		ErrorHandlers []*ErrorHandler           // error handlers expected to be set on the job based on config input
		InitError     error                     // error expected from config validation
		SyncError     error                     // error expected from pipeline.Sync()
		Runs          int                       // number of times the source (and therefore the whole pipeline) was run
		Logged        func(batchSize int) []int // ids of entities that were logged
		ReQueued      []int                     // ids of entities that were requeued
		ProcessedFunc func(batchSize int) int
	}

	////////////////////////// the following types are mock types for testing //////////////////////////
	testSource interface {
		source.Source
	}
	testSampleSource struct {
		source.SampleSource
	}
	failingSource struct {
		testSampleSource
	}
	failingSink struct {
		devNullSink
		recoverAfterRun int
		count           int
		maxBatchSize    int
		succeed         bool
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
	// var dsm *server.DsManager
	var scheduler *Scheduler
	var store *server.Store
	var runner *Runner
	var storeLocation string
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./error_handler_test_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		scheduler, store, runner, _, // dsm,
			_ = setupScheduler(storeLocation)
	})
	AfterEach(func() {
		runner.Stop()
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	DescribeTable("with",

		// This function is the evaluation function, called for every Entry in the table
		func(input tableInput, behaviour expected) {
			// first, we set up the job config
			errorHandlers := ""

			// using random batchsize with total dataset size of 10 (default)
			rndBatchSize := rand.Intn(9) + 2
			//rndBatchSize = 11
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

			// 4th we prepare a pipeline with table entry overrides if given
			jobs, err := scheduler.toTriggeredJobs(j)
			Expect(err).To(BeNil(), tag)
			Expect(jobs).To(HaveLen(1), tag)

			if input.source != nil {
				jobs[0].pipeline.(*IncrementalPipeline).source = input.source
			} else {
				defaultSource := &testSampleSource{SampleSource: source.SampleSource{NumberOfEntities: 10, Store: store}}
				jobs[0].pipeline.(*IncrementalPipeline).source = defaultSource
				input.source = defaultSource
			}
			if input.sink != nil {
				jobs[0].pipeline.(*IncrementalPipeline).sink = input.sink
			}
			if input.transform != nil {
				jobs[0].pipeline.(*IncrementalPipeline).transform = input.transform
			}

			cntP := &countingPipeline{p: jobs[0].pipeline}
			jobs[0].pipeline = cntP
			// 5th we run the pipeline
			jobs[0].Run()

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
								//close(cntP.runChan)
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
			if behaviour.ProcessedFunc != nil {
				expectedProcessed := behaviour.ProcessedFunc(rndBatchSize)
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
				ProcessedFunc: func(batchSize int) int { return 0 },
			},
		),
		Entry(
			"failing source with log handler should just fail the pipeline",
			tableInput{source: &failingSource{}, onErrorJSON: `[{"errorHandler": "log"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 0}},
				SyncError:     errors.New("failing source"), // expect job status to be failed
				ProcessedFunc: func(batchSize int) int { return 0 },
			},
		),
		Entry(
			"failing source with reQueue handler should just fail the pipeline",
			tableInput{source: &failingSource{}, onErrorJSON: `[{"errorHandler": "requeue"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 0}},
				SyncError:     errors.New("failing source"),
				ProcessedFunc: func(batchSize int) int { return 0 },
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
				ProcessedFunc: func(batchSize int) int { return 0 },
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
				SyncError:     errors.New("failing source"),
				ProcessedFunc: func(batchSize int) int { return 0 },
				Runs:          4, // check the pipeline was run 4 time (one regular + 3 retries)
			},
		),

		////////////////////////// Test the 3 error handler types (and combinations) with failing sink.  /////////////
		Entry("failing sink with no handlers should just fail the pipeline",
			tableInput{sink: &failingSink{}},
			expected{
				ErrorHandlers: []*ErrorHandler{},
				SyncError:     errors.New("failing sink"),
				ProcessedFunc: func(batchSize int) int { return int(math.Min(float64(batchSize), 10.0)) },
			},
		),
		Entry("failing sink with reRun handler should repeat run n times",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 3}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "rerun", MaxRetries: 3, RetryDelay: 1}}, // 5 ns delay
				SyncError:     errors.New("failing sink"),
				Runs:          4,
				ProcessedFunc: func(batchSize int) int { return int(math.Min(float64(batchSize), 10.0)) },
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
				ProcessedFunc: func(batchSize int) int {
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
				ProcessedFunc: func(batchSize int) int { return int(math.Min(math.Max(float64(2), float64(batchSize)), 10)) },
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
			tableInput{sink: &failingSink{},
				onErrorJSON: `[{"errorHandler": "log"},{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 2}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log"}, {Type: "rerun", MaxRetries: 2, RetryDelay: 1}},
				SyncError:     errors.New("failing sink"),
				Runs:          3,
				Logged:        func(batchSize int) []int { return []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} },
			},
		),
		Entry("failing sink with capped log handler and reRun handler",
			tableInput{sink: &failingSink{},
				onErrorJSON: `[{"errorHandler": "log", "maxItems": 5},{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 2}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 5}, {Type: "rerun", MaxRetries: 2, RetryDelay: 1}},
				SyncError:     errors.New("failing sink"),
				Runs:          3,
				Logged: func(batchSize int) []int {
					if batchSize < 6 {
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
				ProcessedFunc: func(batchSize int) int {
					if batchSize > 5 {
						return int(math.Min(float64(batchSize), 10.0))
					}
					return map[int]int{1: 6, 2: 6, 3: 6, 4: 8, 5: 10}[batchSize]
				},
			},
		),
		Entry("failing sink with reQueue handler",
			tableInput{sink: &failingSink{}, onErrorJSON: `[{"errorHandler": "requeue"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue"}},
				SyncError:     errors.New("failing sink"),
				Runs:          1,
				ReQueued:      []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			}),

		// recovering means the sink fails the first attempt, but succeeds on the second attempt
		// we want to make sure that we still log the failed entities the first time around,
		// but also end up in a succeeded state after the second attempt
		Entry("recovering sink with capped log handler and reRun handler",
			// going recursively through a batch of 10 entities in splits means 7 sink probes.
			// on the 8th sink access, it should be the next pipeline run
			tableInput{sink: &failingSink{recoverAfterRun: 1},
				onErrorJSON: `[{"errorHandler": "log", "maxItems": 2},{"errorHandler": "rerun", "retryDelay": 1, "maxRetries": 5}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 2}, {Type: "rerun", MaxRetries: 5, RetryDelay: 1}},
				Runs:          2, // not maxing out reRuns, because sink recovers
				Logged: func(batchSize int) []int {
					return []int{0, 1} // logging 2 entities during first run
				},
				ProcessedFunc: func(batchSize int) int {
					if batchSize < 3 {
						return 8
					} else {
						return 10
					}
				},
			}),

		// single entities in sink fail
		// test next run
		// test reruns

		////////////////////////// Test the 3 error handler types (and combinations) with failing transform.  /////////////
		// failing tranfsorm: all handlers applied
		// bug in transform: all entities fail
		// single entities in transform fail
		// test next run
		// test reruns

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
