package source

import (
	"github.com/mimiro-io/datahub/internal/server"
)

type join struct {
	dataset  string
	property string
	inverse  bool
}

type dependency struct {
	dataset string // name of the dependent dataset
	joins   []join
}

type MultiSource struct {
	mainDataset  string       // the main dataset that is read on initial sync
	dependencies []dependency // the dependency queries
}

func (multiSource *MultiSource) GetConfig() map[string]interface{} {
	panic("implement me")
}

type multiSourceContinuationToken struct {
	dependenciesSince []string //
}

func (multiSource *MultiSource) StartFullSync() {
}

func (multiSource *MultiSource) EndFullSync() {
}

func (multiSource *MultiSource) ReadEntities(since string, batchSize int, processEntities func([]*server.Entity, string) error) error {
	//

	return nil
}
