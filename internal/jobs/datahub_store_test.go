package jobs

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"testing"
	"time"
)

func TestDatahubStore(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("operations on configurations", func() {
		testCnt := 0
		var store *server.Store
		var storeLocation string

		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./testdatahubstore_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			store = setupDatahubStore(storeLocation, t)
		})

		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("should store a configuration", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			err := s.SaveConfiguration("id-1", &scheduler.JobConfiguration{
				Id:    "id-1",
				Title: "hello",
			})
			g.Assert(err).IsNil("it failed creating job configuration")
		})
		g.It("should be able to update an existing configuration", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			_ = s.SaveConfiguration("id-1", &scheduler.JobConfiguration{
				Id:    "id-1",
				Title: "hello",
			})
			config, err := s.GetConfiguration("id-1")
			g.Assert(err).IsNil("it failed fetching job configuration")
			g.Assert(config.Title).Eql("hello")

			config.Title = "Updated title"
			err = s.SaveConfiguration("id-1", config)
			g.Assert(err).IsNil("it failed updating job configuration")

			configs, _ := s.ListConfigurations()
			g.Assert(len(configs)).Eql(1, "duplicate configuration")
			g.Assert(configs[0].Title).Eql("Updated title")

		})
		g.It("should be able to retrieve a stored configuration", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			err := s.SaveConfiguration("id-1", &scheduler.JobConfiguration{
				Id:    "id-1",
				Title: "hello",
			})
			g.Assert(err).IsNil("it failed creating job configuration")

			config, err := s.GetConfiguration("id-1")
			g.Assert(err).IsNil("it failed fetching job configuration")
			g.Assert(config).IsNotNil("it didn't find the config")
			g.Assert(config.Id).Eql("id-1")
			g.Assert(config.Title).Eql("hello")
		})
		g.It("should return a list of configurations", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			_ = s.SaveConfiguration("id-1", &scheduler.JobConfiguration{
				Id:    "id-1",
				Title: "hello1",
			})
			_ = s.SaveConfiguration("id-2", &scheduler.JobConfiguration{
				Id:    "id-2",
				Title: "hello2",
			})

			items, err := s.ListConfigurations()
			g.Assert(err).IsNil("failed getting configurations")
			g.Assert(len(items)).Eql(2)
			g.Assert(items[0].Id).Eql("id-1")
		})
		g.It("should be able to delete a configuration", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			_ = s.SaveConfiguration("id-1", &scheduler.JobConfiguration{
				Id:    "id-1",
				Title: "hello1",
			})
			_ = s.SaveConfiguration("id-2", &scheduler.JobConfiguration{
				Id:    "id-2",
				Title: "hello2",
			})

			items, err := s.ListConfigurations()
			g.Assert(err).IsNil("failed getting configurations")
			g.Assert(len(items)).Eql(2)

			err = s.DeleteConfiguration("id-1")
			g.Assert(err).IsNil("failed deleting configuration")

			items, _ = s.ListConfigurations()
			g.Assert(len(items)).Eql(1)
			g.Assert(items[0].Id).Eql("id-2")

		})
	})

	g.Describe("operations on tasks", func() {
		testCnt := 0
		var store *server.Store
		var storeLocation string

		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./testdatahubstore_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			store = setupDatahubStore(storeLocation, t)
		})

		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("should allow to add tasks", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())

			err := s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-1",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusPlanned,
				Error:  "",
			})
			g.Assert(err).IsNil("failed storing task")
		})
		g.It("should allow to return stored tasks for job", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())

			_ = s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-1",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusPlanned,
				Error:  "",
			})
			_ = s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-2",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusPlanned,
				Error:  "",
			})

			tasks, err := s.GetTasks("id-1")
			g.Assert(err).IsNil("failed fetching tasks for job")
			g.Assert(len(tasks)).Eql(2)
		})
		g.It("should be able to change task status", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())

			_ = s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-1",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusPlanned,
				Error:  "",
			})
			tasks, _ := s.GetTasks("id-1")
			g.Assert(tasks[0].Status).Eql(scheduler.StatusPlanned)

			err := s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-1",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusRunning,
				Error:  "",
			})
			g.Assert(err).IsNil("failed updating task")

			tasks, _ = s.GetTasks("id-1")
			g.Assert(len(tasks)).Eql(1, "duplicated task when updating")
			g.Assert(tasks[0].Status).Eql(scheduler.StatusRunning)
		})
		g.It("should be able to remove job tasks", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())

			_ = s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-1",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusPlanned,
				Error:  "",
			})
			_ = s.SaveTaskState("id-1", &scheduler.TaskState{
				Id:     "task-2",
				Type:   scheduler.TypeTask,
				Status: scheduler.StatusPlanned,
				Error:  "",
			})

			tasks, _ := s.GetTasks("id-1")
			g.Assert(len(tasks)).Eql(2)

			err := s.DeleteTasks("id-1")
			tasks, err = s.GetTasks("id-1")
			g.Assert(err).IsNil("failed to delete tasks")
			g.Assert(len(tasks)).Eql(0)
		})

	})

	g.Describe("operations on history", func() {
		testCnt := 0
		var store *server.Store
		var storeLocation string

		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./testdatahubstore_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			store = setupDatahubStore(storeLocation, t)
		})

		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("should store history", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())

			err := s.SaveJobHistory("id-1", &scheduler.JobHistory{
				JobId:     "id-1",
				Title:     "Job 1",
				State:     "SUCCESS",
				Start:     time.Now(),
				End:       time.Now(),
				LastError: "",
				Tasks:     nil,
			})
			g.Assert(err).IsNil("failed storing history")
		})
		g.It("should store many and return all", func() {
			//g.Timeout(2 * time.Minute)
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			err := s.SaveJobHistory("id-1", &scheduler.JobHistory{
				JobId: "id-1",
				Title: "Job 1",
				State: "SUCCESS",
			})
			g.Assert(err).IsNil("failed first")
			err = s.SaveJobHistory("id-1", &scheduler.JobHistory{
				JobId: "id-1",
				Title: "Job 1",
				State: "FAILURE",
			})
			g.Assert(err).IsNil("failed second")
			err = s.SaveJobHistory("id-2", &scheduler.JobHistory{
				JobId: "id-2",
				Title: "Job 2",
				State: "SUCCESS",
			})
			g.Assert(err).IsNil("failed third")
			items, err := s.ListJobHistory(-1)
			g.Assert(err).IsNil("failed retrieving history")
			g.Assert(len(items)).Eql(2)
			g.Assert(string(items[1].JobId)).Eql("id-2")

		})
		g.It("should store many and return limit", func() {
			g.Timeout(2 * time.Minute)
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			_ = s.SaveJobHistory("id-10", &scheduler.JobHistory{
				JobId: "id-10",
				Title: "Job 1",
				State: "SUCCESS",
				Start: time.Now(),
			})
			_ = s.SaveJobHistory("id-10", &scheduler.JobHistory{
				JobId: "id-10",
				Title: "Job 10",
				State: "FAILURE",
				Start: time.Now(),
			})
			items, err := s.GetJobHistory("id-10", 1)
			g.Assert(err).IsNil("failed fetching 1 history")
			g.Assert(len(items)).Eql(1)
			g.Assert(items[0].State).Eql("FAILURE")
		})
		g.It("should store many, and return latest", func() {
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			_ = s.SaveJobHistory("id-11", &scheduler.JobHistory{
				JobId: "id-11",
				Title: "Job 1",
				State: "SUCCESS",
				Start: time.Now(),
			})
			_ = s.SaveJobHistory("id-11", &scheduler.JobHistory{
				JobId: "id-11",
				Title: "Job 1",
				State: "FAILURE",
				Start: time.Now(),
			})
			items, err := s.GetLastJobHistory("id-11")
			g.Assert(err).IsNil("failed fetching last history")
			g.Assert(len(items)).Eql(1)
			g.Assert(items[0].State).Eql("FAILURE")
		})
		g.It("should allow to remove history for job", func() {
			g.Timeout(2 * time.Minute)
			s := NewDataHubJobStore(store, zap.NewNop().Sugar())
			_ = s.SaveJobHistory("id-10", &scheduler.JobHistory{
				JobId: "id-10",
				Title: "Job 1",
				State: "SUCCESS",
			})
			_ = s.SaveJobHistory("id-10", &scheduler.JobHistory{
				JobId: "id-10",
				Title: "Job 10",
				State: "FAILURE",
			})
			items, _ := s.ListJobHistory(-1)
			g.Assert(len(items)).Eql(1, "failed counting to 1")

			err := s.DeleteJobHistory("id-10")
			g.Assert(err).IsNil("failed deleting history")

			items2, _ := s.GetJobHistory("id-10", -1)
			g.Assert(len(items2)).Eql(0)
		})
	})
}

func setupDatahubStore(storeLocation string, t *testing.T) *server.Store {
	// temp redirect of stdout to swallow some annoying init messages in fx and jobrunner and mockService
	devNull, _ := os.Open("/dev/null")
	oldStd := os.Stdout
	os.Stdout = devNull

	e := &conf.Env{
		Logger:        logger,
		StoreLocation: storeLocation,
	}

	lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))
	store := server.NewStore(lc, e, &statsd.NoOpClient{})

	// undo redirect of stdout after successful init of fx and jobrunner
	os.Stdout = oldStd
	err := lc.Start(context.Background())
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	return store
}
