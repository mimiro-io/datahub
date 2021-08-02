package source

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/mimiro-io/datahub/internal/server"
)

type SlowSource struct {
	BatchSize int
	Sleep     string
}

func (source *SlowSource) DecodeToken(token string) DatasetContinuation {
	result := &StringDatasetContinuation{}
	_ = json.Unmarshal([]byte(token), result)
	return result
}

func (source *SlowSource) EncodeToken(token DatasetContinuation) string {
	result, _ := json.Marshal(token)
	return string(result)
}

func (source *SlowSource) StartFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SlowSource) EndFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SlowSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "SlowSource"
	config["BatchSize"] = source.BatchSize
	config["Sleep"] = source.Sleep

	return config
}

func (source *SlowSource) ReadEntities(since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	// assert sample source namespace

	entities := make([]*server.Entity, source.BatchSize)
	for i := 0; i < source.BatchSize; i++ {
		e := server.NewEntity("test:e-"+strconv.Itoa(i), 0)
		entities[i] = e
	}
	d, err := time.ParseDuration(source.Sleep)
	if err != nil {
		return err
	}
	time.Sleep(d)

	return processEntities(entities, &StringDatasetContinuation{""})
}
