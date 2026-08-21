package scheduler

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
)

func NewNamespaceCleaner(logger *zap.SugaredLogger, store store.BadgerStore, doDelete bool) schedulable {
	cleaner := &NamespaceCleaner{
		badger: store,
		Logger: logger,
		delete: doDelete,
	}

	t := newSchedulableTask("namespace_cleanup", true, logger, func() RunResult {
		ctx, ok := cleaner.beginScan()
		if !ok {
			logger.Info("Namespace cleaner is stopped, skipping scan")
			return RunResult{state: RunResultSuccess, timestamp: time.Now()}
		}
		defer cleaner.endScan()
		logger.Info("Starting namespace cleanup scan, delete=" + fmt.Sprint(doDelete))
		err := cleaner.ScanNamespaceUsage(ctx)
		if err != nil {
			logger.Errorf("failed to scan namespace usage: %v", err)
			return RunResult{state: RunResultFailed, timestamp: time.Now()}
		}
		return RunResult{state: RunResultSuccess, timestamp: time.Now()}
	})

	t.OnStop = func(ctx context.Context) error {
		logger.Info("Stopping namespace cleaner")
		return cleaner.Stop(ctx)
	}
	return t
}

type NamespaceCleaner struct {
	badger  store.BadgerStore
	Logger  *zap.SugaredLogger
	lock    sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool
	delete  bool
}

// beginScan registers a new scan and returns its context. It returns false when Stop
// has been called, so a run dispatched around shutdown never touches the store.
func (nc *NamespaceCleaner) beginScan() (context.Context, bool) {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if nc.stopped {
		return nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	nc.cancel = cancel
	nc.done = make(chan struct{})
	return ctx, true
}

// endScan releases the scan registered by beginScan and unblocks a waiting Stop.
func (nc *NamespaceCleaner) endScan() {
	nc.lock.Lock()
	nc.cancel()
	nc.cancel = nil
	done := nc.done
	nc.done = nil
	nc.lock.Unlock()
	close(done)
}

func (nc *NamespaceCleaner) ScanNamespaceUsage(ctx context.Context) error {
	s := nc.badger.GetDB().NewStream()
	// prefix for badger's own 5-second progress log lines
	s.LogPrefix = "namespace_cleanup.scan"
	foundPrefixes := make(map[string]bool)
	mapLock := sync.RWMutex{}
	var scanned, skipped atomic.Int64

	// Set up prefix filter for entity index to reduce iteration scope
	entityIndexPrefix := make([]byte, 2)
	binary.BigEndian.PutUint16(entityIndexPrefix, ENTITY_ID_TO_JSON_INDEX_ID)
	s.Prefix = entityIndexPrefix

	s.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		// key layout: index id (2) + entity rid (8) + dataset id (4) + ...
		// skip entities of deleted datasets still awaiting GC
		if len(key) >= 14 {
			dsID := types.InternalDatasetID(binary.BigEndian.Uint32(key[10:14]))
			if nc.badger.IsDatasetDeleted(dsID) {
				skipped.Add(1)
				return nil, nil
			}
		}
		scanned.Add(1)
		item := itr.Item()
		if item.IsDeletedOrExpired() {
			return nil, nil
		}

		// Parse entity JSON and extract namespace prefixes directly here
		// to avoid copying large entity values
		var prefixes []string
		err := item.Value(func(val []byte) error {
			extractedPrefixes, err := nc.extractNamespacePrefixesFromJSON(val)
			if err != nil {
				return err
			}
			prefixes = extractedPrefixes
			return nil
		})
		if err != nil {
			nc.Logger.Warnf("failed to parse entity data: %v", err)
			return nil, nil
		}

		// Only emit the extracted prefixes, not the full entity data
		if len(prefixes) > 0 {
			// Encode prefixes as JSON for transmission
			prefixesJSON, err := json.Marshal(prefixes)
			if err != nil {
				return nil, err
			}

			kv := &pb.KV{
				Key:   item.Key(), // We still need a key for the KV structure
				Value: prefixesJSON,
			}
			return &pb.KVList{
				Kv: []*pb.KV{kv},
			}, nil
		}

		return nil, nil
	}

	s.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		// Collect all prefixes from this batch
		batchPrefixes := make(map[string]bool)
		for _, kv := range list.Kv {
			if kv.StreamDone {
				break
			}

			// Decode the prefix slice from JSON
			var prefixes []string
			if err := json.Unmarshal(kv.Value, &prefixes); err != nil {
				nc.Logger.Warnf("failed to unmarshal prefixes: %v", err)
				continue
			}

			// Add to batch collection
			for _, prefix := range prefixes {
				batchPrefixes[prefix] = true
			}
		}

		// Merge batch prefixes into global collection with lock
		mapLock.Lock()
		for prefix := range batchPrefixes {
			foundPrefixes[prefix] = true
		}
		mapLock.Unlock()

		return nil
	}

	err := s.Orchestrate(ctx)
	if err != nil {
		return fmt.Errorf("failed to orchestrate stream: %w", err)
	}

	// Orchestrate can return nil when cancellation stops the scan early, and the
	// prefixes collected so far are incomplete
	if ctx.Err() != nil {
		return ctx.Err()
	}

	nc.Logger.Infof("Namespace cleanup scanned %d entity versions, skipped %d versions from deleted datasets",
		scanned.Load(), skipped.Load())

	// Compare found prefixes with stored prefixes
	nc.compareWithStoredPrefixes(foundPrefixes)
	return nil
}

func (nc *NamespaceCleaner) extractNamespacePrefixesFromJSON(entityData []byte) ([]string, error) {
	var entity map[string]interface{}
	err := json.Unmarshal(entityData, &entity)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal entity: %w", err)
	}

	// Use a map to collect unique prefixes
	prefixMap := make(map[string]bool)
	nc.collectEntityPrefixes(entity, prefixMap)

	// Convert map to slice
	prefixes := make([]string, 0, len(prefixMap))
	for prefix := range prefixMap {
		prefixes = append(prefixes, prefix)
	}

	return prefixes, nil
}

// collectEntityPrefixes gathers prefixes from an entity's id, property keys, and
// reference keys and values, recursing into sub-entities in property values.
func (nc *NamespaceCleaner) collectEntityPrefixes(entity map[string]interface{}, prefixMap map[string]bool) {
	// Extract prefix from entity ID
	if id, ok := entity["id"].(string); ok {
		if prefix := nc.extractPrefix(id); prefix != "" {
			prefixMap[prefix] = true
		}
	}

	// Extract prefixes from property keys, and recurse into sub-entities
	if props, ok := entity["props"].(map[string]interface{}); ok {
		for key, value := range props {
			if prefix := nc.extractPrefix(key); prefix != "" {
				prefixMap[prefix] = true
			}
			nc.collectSubEntityPrefixes(value, prefixMap)
		}
	}

	// Extract prefixes from reference keys and values
	if refs, ok := entity["refs"].(map[string]interface{}); ok {
		for key, value := range refs {
			// Extract from reference key
			if prefix := nc.extractPrefix(key); prefix != "" {
				prefixMap[prefix] = true
			}

			// Extract from reference value(s)
			switch v := value.(type) {
			case string:
				if prefix := nc.extractPrefix(v); prefix != "" {
					prefixMap[prefix] = true
				}
			case []interface{}:
				for _, item := range v {
					if str, ok := item.(string); ok {
						if prefix := nc.extractPrefix(str); prefix != "" {
							prefixMap[prefix] = true
						}
					}
				}
			}
		}
	}
}

// collectSubEntityPrefixes recurses into property values that hold sub-entities.
func (nc *NamespaceCleaner) collectSubEntityPrefixes(value interface{}, prefixMap map[string]bool) {
	switch v := value.(type) {
	case map[string]interface{}:
		nc.collectEntityPrefixes(v, prefixMap)
	case []interface{}:
		for _, item := range v {
			nc.collectSubEntityPrefixes(item, prefixMap)
		}
	}
}

func (nc *NamespaceCleaner) extractPrefix(value string) string {
	if strings.Contains(value, ":") {
		parts := strings.SplitN(value, ":", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			// Skip common URI schemes that are not namespace prefixes
			if parts[0] == "http" || parts[0] == "https" || parts[0] == "urn" {
				return ""
			}
			return parts[0]
		}
	}
	return ""
}

func (nc *NamespaceCleaner) compareWithStoredPrefixes(foundPrefixes map[string]bool) {
	// Get stored prefixes from namespace manager
	storedPrefixes := nc.badger.GetAllNamespacePrefixes()

	// Find prefixes that are stored but not used
	unusedPrefixes := make(map[string]string)
	for storedPrefix, expansion := range storedPrefixes {
		if !foundPrefixes[storedPrefix] {
			unusedPrefixes[storedPrefix] = expansion
		}
	}

	// Find prefixes that are used but not stored (shouldn't happen in normal operation)
	missingPrefixes := []string{}
	for foundPrefix := range foundPrefixes {
		if _, exists := storedPrefixes[foundPrefix]; !exists {
			missingPrefixes = append(missingPrefixes, foundPrefix)
		}
	}

	// Log the results
	nc.Logger.Infof("Namespace cleanup scan completed:")
	nc.Logger.Infof("  Total stored prefixes: %d", len(storedPrefixes))
	nc.Logger.Infof("  Total found prefixes: %d", len(foundPrefixes))
	nc.Logger.Infof("  Unused stored prefixes: %d", len(unusedPrefixes))
	nc.Logger.Infof("  Missing prefixes: %d", len(missingPrefixes))

	if len(unusedPrefixes) > 0 {
		nc.Logger.Infof("Unused prefixes:")
		for prefix, expansion := range unusedPrefixes {
			nc.Logger.Infof("  %s -> %s", prefix, expansion)
		}

		// Delete unused prefixes if doDelete flag is true
		if nc.delete {
			nc.Logger.Infof("Deleting %d unused namespace prefixes", len(unusedPrefixes))
			for prefix := range unusedPrefixes {
				err := nc.badger.DeleteNamespacePrefix(prefix)
				if err != nil {
					nc.Logger.Errorf("Failed to delete namespace prefix %s: %v", prefix, err)
				} else {
					nc.Logger.Infof("Deleted namespace prefix: %s", prefix)
				}
			}
		}
	}

	if len(missingPrefixes) > 0 {
		nc.Logger.Warnf("Missing prefixes (used but not stored): %v", missingPrefixes)
	}
}

// Stop cancels a running scan and waits for it to unwind, so that shutdown can
// close the store without the scan still reading from it. Later runs skip the scan.
func (nc *NamespaceCleaner) Stop(_ context.Context) error {
	nc.lock.Lock()
	nc.stopped = true
	cancel, done := nc.cancel, nc.done
	nc.lock.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return nil
}
