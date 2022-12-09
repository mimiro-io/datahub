package jobs

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"go.uber.org/zap"
	"sort"
)

type DataHubJobStore struct {
	store  *server.Store
	logger *zap.SugaredLogger
}

func NewDataHubJobStore(store *server.Store, logger *zap.SugaredLogger) scheduler.Store {
	return &DataHubJobStore{
		store:  store,
		logger: logger.Named("scheduler"),
	}
}

func (store *DataHubJobStore) GetConfiguration(id string) (*scheduler.JobConfiguration, error) {
	jobConfig := &scheduler.JobConfiguration{}
	err := store.store.GetObject(server.SCHEDULER_JOB_CONFIG_INDEX, id, jobConfig)
	return jobConfig, err
}

func (store *DataHubJobStore) SaveConfiguration(id string, config *scheduler.JobConfiguration) error {
	return store.store.StoreObject(server.SCHEDULER_JOB_CONFIG_INDEX, id, config)
}

func (store *DataHubJobStore) DeleteConfiguration(id string) error {
	return store.store.DeleteObject(server.SCHEDULER_JOB_CONFIG_INDEX, id)
}

func (store *DataHubJobStore) ListConfigurations() ([]*scheduler.JobConfiguration, error) {
	jobConfigs := make([]*scheduler.JobConfiguration, 0)
	err := store.store.IterateObjectsRaw(server.SCHEDULER_JOB_CONFIG_INDEX_BYTES, func(jsonData []byte) error {
		jobConfig := &scheduler.JobConfiguration{}
		err := json.Unmarshal(jsonData, jobConfig)

		if err != nil {
			store.logger.Warnf(" > Error parsing job from store - aborting start: %s", err)
			return err
		}
		jobConfigs = append(jobConfigs, jobConfig)

		return nil
	})
	return jobConfigs, err
}

func (store *DataHubJobStore) GetTaskState(jobId scheduler.JobId, taskId string) (*scheduler.TaskState, error) {
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	task := &scheduler.TaskState{}
	err := store.store.GetObject(server.SCHEDULER_JOB_STATE_INDEX, id, task)
	return task, err
}

func (store *DataHubJobStore) SaveTaskState(jobId scheduler.JobId, state *scheduler.TaskState) error {
	id := fmt.Sprintf("%s::%s", jobId, state.Id)
	return store.store.StoreObject(server.SCHEDULER_JOB_STATE_INDEX, id, state)
}

func (store *DataHubJobStore) GetTasks(jobId scheduler.JobId) ([]*scheduler.TaskState, error) {
	id := fmt.Sprintf("::%s::", jobId)
	prefix := append(server.SCHEDULER_JOB_STATE_INDEX_BYTES, []byte(id)...)

	tasks := make([]*scheduler.TaskState, 0)
	err := store.store.IterateObjectsRaw(prefix, func(jsonData []byte) error {
		task := &scheduler.TaskState{}
		err := json.Unmarshal(jsonData, task)

		if err != nil {
			store.logger.Warnf(" > Error parsing job from store - aborting start: %s", err)
			return err
		}
		tasks = append(tasks, task)

		return nil
	})
	return tasks, err
}

func (store *DataHubJobStore) DeleteTasks(jobId scheduler.JobId) error {
	all, err := store.GetTasks(jobId)
	if err != nil {
		return err
	}
	for _, item := range all { // this is not optimal, but should work
		err := store.store.DeleteObject(server.SCHEDULER_JOB_STATE_INDEX, fmt.Sprintf("%s::%s", jobId, item.Id))
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *DataHubJobStore) GetLastJobHistory(jobId scheduler.JobId) ([]*scheduler.JobHistory, error) {
	return store.GetJobHistory(jobId, 1)
}

func (store *DataHubJobStore) GetJobHistory(jobId scheduler.JobId, limit int) ([]*scheduler.JobHistory, error) {
	history := &scheduler.JobHistory{}
	err := store.store.GetObject(server.SCHEDULER_JOB_LOG_INDEX, string(jobId), history)
	if err != nil {
		return nil, err
	}
	if history.JobId == "" {
		return nil, nil
	}
	return []*scheduler.JobHistory{
		history,
	}, nil

}

func (store *DataHubJobStore) ListJobHistory(limit int) ([]*scheduler.JobHistory, error) {
	prefix := append(server.SCHEDULER_JOB_LOG_INDEX_BYTES, []byte("::")...)

	tasks := make([]*scheduler.JobHistory, 0)
	err := store.store.IterateObjectsRaw(prefix, func(jsonData []byte) error {
		task := &scheduler.JobHistory{}
		err := json.Unmarshal(jsonData, task)

		if err != nil {
			store.logger.Warnf(" > Error parsing history from store - aborting start: %s", err)
			return err
		}
		tasks = append(tasks, task)

		return nil
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Start.Unix() < tasks[j].Start.Unix()
	})

	if limit > -1 && len(tasks) > limit {
		return tasks[len(tasks)-limit:], nil
	}
	return tasks, nil
}

func (store *DataHubJobStore) SaveJobHistory(jobId scheduler.JobId, history *scheduler.JobHistory) error {
	runId, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	id := fmt.Sprintf("%s::%s", jobId, runId)
	history.Id = id
	return store.store.StoreObject(server.SCHEDULER_JOB_LOG_INDEX, string(jobId), history)
}

func (store *DataHubJobStore) DeleteJobHistory(jobId scheduler.JobId) error {
	return store.store.DeleteObject(server.SCHEDULER_JOB_LOG_INDEX, string(jobId))
}

var _ scheduler.Store = (*DataHubJobStore)(nil)
