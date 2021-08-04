package source

import (
	"strconv"

	"github.com/mimiro-io/datahub/internal/server"
)

type SampleSource struct {
	NumberOfEntities int
	BatchSize        int
	Store            *server.Store
}

func (source *SampleSource) DecodeToken(token string) DatasetContinuation {
	return &StringDatasetContinuation{token}
}

func (source *SampleSource) EncodeToken(token DatasetContinuation) string {
	return token.GetToken()
}

func (source *SampleSource) StartFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SampleSource) EndFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SampleSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "SampleSource"
	config["NumberOfEntities"] = source.NumberOfEntities
	return config
}

func (source *SampleSource) ReadEntities(since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {

	var err error
	sinceOffset := int(since.AsIncrToken())
	if source.NumberOfEntities < batchSize {
		batchSize = source.NumberOfEntities
	}
	if sinceOffset >= source.NumberOfEntities {
		batchSize = 0
	}

	// assert sample source namespace
	prefix, err := source.Store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.samplesource.org/")
	if err != nil {
		return err
	}

	endIndex := sinceOffset + batchSize

	entities := make([]*server.Entity, 0)
	for i := sinceOffset; i < endIndex; i++ {
		e := server.NewEntity(prefix+":e-"+strconv.Itoa(i), 0)
		entities = append(entities, e)
	}

	sinceToken := &StringDatasetContinuation{strconv.Itoa(endIndex)}
	return processEntities(entities, sinceToken)
}
