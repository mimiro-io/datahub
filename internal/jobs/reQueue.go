package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

type reQueue struct {
	jobID string
	ds    *server.Dataset
}

func newReQueue(id string, dsm *server.DsManager) (*reQueue, error) {
	ds, err := dsm.CreateDataset(fmt.Sprintf("%v_requeue_%v", id, time.Now().UnixNano()), nil)
	if err != nil {
		return nil, err
	}
	return &reQueue{
		jobID: id,
		ds:    ds,
	}, nil
}

func (q *reQueue) enQueue(entity *server.Entity) error {
	ent := server.NewEntity(fmt.Sprintf("%v-%v", entity.ID, "requeue"), 0)
	ent.Properties["content"] = entity
	return q.ds.StoreEntities([]*server.Entity{ent})
}

type reQueuePrependingSource struct {
	jobId     string
	s         source.Source
	dsm       *server.DsManager
	prepended bool
}

func (r *reQueuePrependingSource) GetConfig() map[string]interface{} {
	return r.s.GetConfig()
}

func (r *reQueuePrependingSource) ReadEntities(ctx context.Context, since source.DatasetContinuation, batchSize int, processEntities func([]*server.Entity, source.DatasetContinuation) error) error {
	if !r.prepended {
		r.prepended = true
		for _, dsn := range r.dsm.GetDatasetNames() {
			if strings.HasPrefix(dsn.Name, fmt.Sprintf("%v_requeue_", r.jobId)) {
				reQueueDs := r.dsm.GetDataset(dsn.Name)
				var batch []*server.Entity
				var reQueueToken uint64 = 0
				for {
					batch = []*server.Entity{}
					next, err := reQueueDs.ProcessChanges(reQueueToken, batchSize, false, func(entity *server.Entity) {
						if m, ok := entity.Properties["content"].(map[string]any); ok {
							e := server.NewEntityFromMap(m)
							batch = append(batch, e)
						}
					})
					reQueueToken = next
					if len(batch) == 0 {
						break
					}
					if err != nil {
						return err
					}
					err = processEntities(batch, &source.StringDatasetContinuation{})
					if err != nil {
						return err
					}
				}
				// delete requeue when done. deliberately not as defer/guaranteed delete.
				// it is better to reprocess twice than to lose queue when something unforeseen happens during processing
				err := r.dsm.DeleteDataset(dsn.Name)
				if err != nil {
					return err
				}
			}
		}
	}
	return r.s.ReadEntities(ctx, since, batchSize, processEntities)
}

func (r *reQueuePrependingSource) StartFullSync() {
	r.s.StartFullSync()
}

func (r *reQueuePrependingSource) EndFullSync() {
	r.s.EndFullSync()
}