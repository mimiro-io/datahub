package jobs

import (
	"errors"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

type (
	tableInput struct {
		onErrorJSON string
		source      testSource
		sink        Sink
	}
	expected struct {
		ErrorHandlers []*ErrorHandler
		InitError     error
		Count         int
		SyncError     error
		SourceRuns    int
	}
	testSource interface {
		source.Source
		channel() chan bool
	}
	testSampleSource struct {
		source.SampleSource
		runChan chan bool
	}
	failingSource struct {
		testSampleSource
	}
)

func (fs *failingSource) ReadEntities(cont source.DatasetContinuation, limit int,
	f func(entities []*server.Entity, continuation source.DatasetContinuation) error,
) error {
	if fs.runChan == nil {
		fs.runChan = make(chan bool, 1000)
	}
	fs.runChan <- true
	return errors.New("failing source")
}

func (tss *testSampleSource) ReadEntities(cont source.DatasetContinuation, limit int,
	f func(entities []*server.Entity, continuation source.DatasetContinuation) error,
) error {
	if tss.runChan == nil {
		tss.runChan = make(chan bool, 1000)
	}
	// only count first source batch
	if cont.GetToken() == "" {
		tss.runChan <- true
	}
	return tss.SampleSource.ReadEntities(cont, limit, f)
}

func (tss *testSampleSource) channel() chan bool {
	return tss.runChan
}

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
	DescribeTable(
		"with",

		// This function is the evaluation function, called for every Entry in the table
		func(input tableInput, behaviour expected) {
			// first, we set up the job config
			errorHandlers := ""
			tag := fmt.Sprintf("(input was %+v)", input)
			if input.onErrorJSON != "" {
				errorHandlers = fmt.Sprintf(`,"onError": %v`, input.onErrorJSON)
			}
			jobConfig := fmt.Sprintf(`{
				"id": "test",
				"title": "test",
				"triggers": [{
				"triggerType": "cron",
				"jobType": "incremental",
				"schedule": "@every 2s"%v
				}],
				"paused": true,
				"source" : { "Type" : "SampleSource", "NumberOfEntities": 10 },
				"sink" : { "Type" : "DevNullSink" }
				}`, errorHandlers)

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
			Expect(j.Triggers[0].ErrorHandlers).To(BeEquivalentTo(behaviour.ErrorHandlers), tag)

			// 4th we prepare a pipeline with table entry overrides if given
			jobs, err := scheduler.toTriggeredJobs(j)
			Expect(err).To(BeNil(), tag)
			Expect(jobs).To(HaveLen(1), tag)

			if input.source != nil {
				jobs[0].pipeline.(*IncrementalPipeline).source = input.source
			} else {
				defaultSource := &testSampleSource{}
				defaultSource.Store = store
				defaultSource.NumberOfEntities = 10
				jobs[0].pipeline.(*IncrementalPipeline).source = defaultSource
				input.source = defaultSource
			}
			if input.sink != nil {
				jobs[0].pipeline.(*IncrementalPipeline).sink = input.sink
			}

			// 5th we run the pipeline
			jobs[0].Run()

			// after the run, fetch job result from badger
			r := jobResult{}
			err = store.GetObject(server.JobResultIndex, jobs[0].id, &r)
			Expect(err).To(BeNil(), tag)
			err = nil
			if r.LastError != "" {
				err = errors.New(r.LastError)
			}
			processed := r.Processed
			if behaviour.SyncError != nil {
				Expect(err).To(BeEquivalentTo(behaviour.SyncError), tag)
			} else {
				Expect(err).To(BeNil(), tag)
			}
			if behaviour.Count != 0 {
				// -1 means nothing was processed.
				// cannot use 0 to denote nothing was processed, because 0 is the default value for int and means "nothing specified".
				if behaviour.Count == -1 {
					Expect(processed).To(BeEquivalentTo(0), tag)
				} else {
					Expect(processed).To(BeEquivalentTo(behaviour.Count), tag)
				}
			} else {
				// default sampleSource produces 10 entities
				Expect(processed).To(Equal(10), tag)
			}

			if behaviour.SourceRuns != 0 {
				cnt := 0
				func() {
					for {
						if input.source == nil {
							return
						}
						select {
						case <-input.source.channel():
							cnt += 1
						// give it some time to do all retries. timing based tests are not ideal but works for now
						case <-time.After(100 * time.Millisecond):
							close(input.source.channel())
							return
						}
					}
				}()
				Expect(cnt).To(Equal(behaviour.SourceRuns), tag)
			}
		},

		// This function generates the description of the Entry if not provided explicitly
		func(input tableInput, behaviour expected) string {
			return fmt.Sprintf("should accept %v and do %v ", input, behaviour)
		},

		////////////////////////// First bunch of Entries tests configuration validation //////////////////////////
		Entry("no handlers", tableInput{onErrorJSON: ""}, expected{
			ErrorHandlers: nil,
			InitError:     nil,
		}),
		Entry("empty handlers", tableInput{onErrorJSON: "[]"}, expected{
			ErrorHandlers: []*ErrorHandler{},
			InitError:     nil,
		}),
		Entry("empty handler object", tableInput{onErrorJSON: "[{}]"}, expected{
			ErrorHandlers: nil,
			InitError:     errors.New("need to set 'errorHandler'. must be one of: reRun, reQueue, log"),
		}),
		Entry("missing errorHandler in handler object", tableInput{onErrorJSON: `[{"maxItems": 5}]`}, expected{
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
		Entry(
			"capital spelling should be accepted, and default values should be set",
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
				SourceRuns:    1, // reRun configured, but no failures, so job should run only once
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
				SyncError: errors.New("failing source"),
				Count:     -1, // -1 means no entities processed, hard to use 0 since it's the default value for int and means "no set"
			},
		),
		Entry(
			"failing source with log handler should just fail the pipeline",
			tableInput{source: &failingSource{}, onErrorJSON: `[{"errorHandler": "log"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "log", MaxItems: 0}},
				SyncError:     errors.New("failing source"), // expect job status to be failed
				Count:         -1,                           // -1 means no entities processed
			},
		),
		Entry(
			"failing source with reQueue handler should just fail the pipeline",
			tableInput{source: &failingSource{}, onErrorJSON: `[{"errorHandler": "requeue"}]`},
			expected{
				ErrorHandlers: []*ErrorHandler{{Type: "requeue", MaxItems: 0}},
				SyncError:     errors.New("failing source"),
				Count:         -1, // -1 means no entities processed
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
				Count:         -1, // -1 means no entities processed
				SourceRuns:    4,  // check the pipeline was run 4 time (one regular + 3 retries)
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
					{Type: "rerun", MaxRetries: 3, RetryDelay: 1},
					{Type: "log"},
					{Type: "requeue"},
				}, // 5 ns delay
				SyncError:  errors.New("failing source"),
				Count:      -1, // -1 means no entities processed
				SourceRuns: 4,  // check the pipeline was run 4 time (one regular + 3 retries)
			},
		),

		////////////////////////// Test the 3 error handler types (and combinations) with failing transform.  /////////////
		// failing tranfsorm or failing sink: all handlers applied
		// bug in transform: all entities fail
		// single entities in transform fail
		// test next run
		// test reruns

		////////////////////////// Test the 3 error handler types (and combinations) with failing sink.  /////////////
		// sink down: cannot detect entity
		// single entities in sink fail
		// test next run
		// test reruns

		// cancelled job should not retry, how to test?
	)
})
