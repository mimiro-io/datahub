// Copyright 2021 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

type qresult struct {
	PredicateID uint64
	EntityID    uint64
	Time        uint64
	DatasetID   uint32
}

// Store data structure
type Store struct {
	database             *badger.DB
	datasets             *sync.Map              // concurrent map of datasets
	datasetsByInternalID *sync.Map              // concurrent map of datasets
	deletedDatasets      map[uint32]bool        // list of datasets to be garbage collected
	storeLocation        string                 // local location of data folder
	nextDatasetID        uint32                 // the next internal id for a dataset
	NamespaceManager     *NamespaceManager      // namespace manager for consistent short expansions
	logger               *zap.SugaredLogger     // store logger
	statsdClient         statsd.ClientInterface // datadog metrics
	idseq                *badger.Sequence       // sequence used for assigning ids to uris
	idtxn                *badger.Txn            // rolling txn for ids
	idmux                sync.Mutex
	fullsyncLeaseTimeout time.Duration
	blockCacheSize       int64
	valueLogFileSize     int64
	maxCompactionLevels  int
	SlowLogThreshold     time.Duration
}

type BadgerLogger struct { // we use this to implement the Badger Logger interface
	Logger *zap.SugaredLogger
}

func (bl BadgerLogger) Errorf(format string, v ...interface{}) { bl.Logger.Errorf(format, v...) }
func (bl BadgerLogger) Infof(format string, v ...interface{}) {
	// find parent in call stack
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc).Name()
	if f == "github.com/dgraph-io/badger/v4.(*Stream).produceRanges" {
		// a bit noisy this one
		return
	}
	bl.Logger.Infof(format, v...)
}
func (bl BadgerLogger) Warningf(format string, v ...interface{}) { bl.Logger.Warnf(format, v...) }
func (bl BadgerLogger) Debugf(format string, v ...interface{})   { bl.Logger.Debugf(format, v...) }

// NewStore Create a new store
func NewStore(env *conf.Config, statsdClient statsd.ClientInterface) *Store {
	fsTimeout := env.FullsyncLeaseTimeout
	if fsTimeout == 0*time.Second {
		env.Logger.Warnf("No fullsync lease timeout set, fallback to 1h")
		fsTimeout = 1 * time.Hour
	}
	store := &Store{
		datasets:             &sync.Map{},
		datasetsByInternalID: &sync.Map{},
		deletedDatasets:      make(map[uint32]bool),
		storeLocation:        env.StoreLocation,
		logger:               env.Logger.Named("store"),
		statsdClient:         statsdClient,
		fullsyncLeaseTimeout: fsTimeout,
		blockCacheSize:       env.BlockCacheSize,
		valueLogFileSize:     env.ValueLogFileSize,
		maxCompactionLevels:  env.MaxCompactionLevels,
		SlowLogThreshold:     env.SlowLogThreshold,
	}
	store.NamespaceManager = NewNamespaceManager(store)

	store.logger.Infof("Opening store in location: %s", store.storeLocation)
	err := store.Open()
	if err != nil {
		store.logger.Fatalf("Unable to open store %s due to %s ", store.storeLocation, err.Error())
	}

	/* lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			store.logger.Infof("Opening store in location: %s", store.storeLocation)
			return store.Open()
		},
		OnStop: func(_ context.Context) error {
			store.logger.Infof("Unmounting store")
			return store.Close()
		},
	}) */

	return store
}

/*func (s *Store) Start(ctx context.Context) error {
	s.logger.Infof("Opening store in location: %s", s.storeLocation)
	return s.Open()
}

func (s *Store) Stop(ctx context.Context) error {
	s.logger.Infof("Closing store")
	return s.Close()
}*/

func (s *Store) Delete() error {
	err := s.Close()
	if err != nil {
		return err
	}
	err = os.RemoveAll(s.storeLocation)
	if err != nil {
		return err
	}
	s.deletedDatasets = make(map[uint32]bool)
	s.datasets = &sync.Map{}
	err = s.Open()
	if err != nil {
		return err
	}
	return nil
}

func NewNamespaceManager(store *Store) *NamespaceManager {
	nsm := &NamespaceManager{}
	nsm.store = store
	nsm.expansionToPrefixMapping = make(map[string]string)
	nsm.prefixToExpansionMapping = make(map[string]string)
	return nsm
}

type NamespaceManager struct {
	lock                     sync.Mutex
	prefixToExpansionMapping map[string]string
	expansionToPrefixMapping map[string]string
	store                    *Store
}

type NamespacesState struct {
	PrefixToExpansionMapping map[string]string
	ExpansionToPrefixMapping map[string]string
}

type Context struct {
	ID         string            `json:"id"`
	Namespaces map[string]string `json:"namespaces"`
}

// GetContext return a context instance
//
//	containing namespace mappings for the given list of namespace prefixes
//	if the give list of given namespace prefixes is empty, all namespace mappings are returned
func (namespaceManager *NamespaceManager) GetContext(includedNamespaces []string) (context *Context) {
	if len(includedNamespaces) > 0 {
		filteredPrefixMapping := map[string]string{}
		for _, expansionURI := range includedNamespaces {
			prefix, _ := namespaceManager.GetPrefixMappingForExpansion(expansionURI)
			filteredPrefixMapping[prefix] = expansionURI
		}
		context = &Context{ID: "@context", Namespaces: filteredPrefixMapping}
	} else {
		context = &Context{ID: "@context", Namespaces: namespaceManager.GetPrefixToExpansionMap()}
	}
	return
}

func (namespaceManager *NamespaceManager) ExpandCurie(curie string) (string, error) {
	splitOffset := strings.Index(curie, ":")
	if splitOffset == -1 {
		return "", errors.New("not a valid curie: " + curie)
	}
	prefix := curie[:splitOffset]
	postfix := curie[splitOffset+1:]
	namespaceManager.lock.Lock()
	expansion, ok := namespaceManager.prefixToExpansionMapping[prefix]
	namespaceManager.lock.Unlock()
	if ok {
		return expansion + postfix, nil
	}
	return "", errors.New("Could not expand curie, unable to find expansion for prefix : " + prefix)
}

func (namespaceManager *NamespaceManager) GetPrefixMappingForExpansion(uriExpansion string) (string, error) {
	namespaceManager.lock.Lock()
	prefix, ok := namespaceManager.expansionToPrefixMapping[uriExpansion]
	namespaceManager.lock.Unlock()
	if ok {
		return prefix, nil
	}
	return "", errors.New("Could not get prefix for unknown URI expansion: " + uriExpansion)
}

func (namespaceManager *NamespaceManager) GetPrefixToExpansionMap() (result map[string]string) {
	namespaceManager.lock.Lock()
	result = namespaceManager.prefixToExpansionMapping
	namespaceManager.lock.Unlock()
	return
}

func (namespaceManager *NamespaceManager) AssertPrefixMappingForExpansion(uriExpansion string) (string, error) {
	namespaceManager.lock.Lock()
	defer namespaceManager.lock.Unlock()

	prefix := namespaceManager.expansionToPrefixMapping[uriExpansion]
	if prefix == "" {
		prefix = "ns" + strconv.Itoa(len(namespaceManager.prefixToExpansionMapping))
		namespaceManager.prefixToExpansionMapping[prefix] = uriExpansion
		namespaceManager.expansionToPrefixMapping[uriExpansion] = prefix
		state := &NamespacesState{}
		state.PrefixToExpansionMapping = namespaceManager.prefixToExpansionMapping
		state.ExpansionToPrefixMapping = namespaceManager.expansionToPrefixMapping
		err := namespaceManager.store.StoreObject(NamespacesIndex, "namespacestate", state)
		if err != nil {
			return "", err
		}
	}
	return prefix, nil
}

type DsNsInfo struct {
	DatasetPrefix       string
	PublicNamespacesKey string
	NameKey             string
	ItemsKey            string
}

func (namespaceManager *NamespaceManager) GetDatasetNamespaceInfo() (DsNsInfo, error) {
	prefix, err := namespaceManager.GetPrefixMappingForExpansion("http://data.mimiro.io/core/dataset/")
	if err != nil {
		return DsNsInfo{}, err
	}

	return DsNsInfo{
		DatasetPrefix: prefix, PublicNamespacesKey: prefix + ":publicNamespaces",
		NameKey: prefix + ":name", ItemsKey: prefix + ":items",
	}, nil
}

func getURLParts(url string) (string, string, error) {
	index := strings.LastIndex(url, "#")
	if index > -1 {
		return url[:index+1], url[index+1:], nil
	}

	index = strings.LastIndex(url, "/")
	if index > -1 {
		return url[:index+1], url[index+1:], nil
	}

	return "", "", errors.New("unable to split url") // fixme do something better
}

func (s *Store) ExpandCurie(curie string) (string, error) {
	return s.NamespaceManager.ExpandCurie(curie)
}

func (s *Store) GetNamespacedIdentifierFromURI(val string) (string, error) {
	if strings.HasPrefix(val, "http://") || strings.HasPrefix(val, "https://") {
		expansion, lastPathPart, err := getURLParts(val)
		if err != nil {
			return "", err
		}
		prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(expansion)
		if err != nil {
			return "", err
		}
		return prefix + ":" + lastPathPart, nil
	}

	return "", errors.New("unable to create namespaced identifier from uri")
}

func (s *Store) GetNamespacedIdentifier(val string, localNamespaces map[string]string) (string, error) {
	if val == "" {
		return "", errors.New("empty value not allowed")
	}

	if strings.HasPrefix(val, "http://") {
		expansion, lastPathPart, err := getURLParts(val)
		if err != nil {
			return "", err
		}

		// check for global expansion
		prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(expansion)
		if err != nil {
			return "", nil
		}
		return prefix + ":" + lastPathPart, nil
	}

	if strings.HasPrefix(val, "https://") {
		expansion, lastPathPart, err := getURLParts(val)
		if err != nil {
			return "", err
		}

		// check for global expansion
		prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(expansion)
		if err != nil {
			return "", err
		}
		return prefix + ":" + lastPathPart, nil
	}

	indexOfColon := strings.Index(val, ":")
	if indexOfColon == -1 {
		localExpansion := localNamespaces["_"]
		if localExpansion == "" {
			return "", errors.New("no expansion for default prefix _ ")
		}

		prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(localExpansion)
		if err != nil {
			return "", err
		}
		return prefix + ":" + val, nil

	} else {
		localPrefix := val[:indexOfColon]
		lastPathPart := val[indexOfColon+1:]
		localExpansion := localNamespaces[localPrefix]

		if localExpansion == "" {
			return "", errors.New("no expansion for prefix " + localPrefix)
		}

		prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(localExpansion)
		if err != nil {
			return "", err
		}

		return prefix + ":" + lastPathPart, nil
	}
}

func (s *Store) GetGlobalContext(strict bool) *Context {
	completeCtx := s.NamespaceManager.GetContext(nil)
	if !strict {
		return completeCtx
	}
	// TODO: consider caching this. Currently GetGlobalContext is only called once per request so it's not called too often
	filterdCtx := &Context{ID: "@context", Namespaces: make(map[string]string)}
	for prefix, expansion := range completeCtx.Namespaces {
		if strings.HasSuffix(expansion, "#") || strings.HasSuffix(expansion, "/") {
			filterdCtx.Namespaces[prefix] = expansion
		}
	}
	return filterdCtx
}

// Open Opens the store. Must be called before using.
func (s *Store) Open() error {
	s.logger.Info("Open database")
	opts := badger.DefaultOptions(s.storeLocation)

	if s.maxCompactionLevels > 0 {
		// default is 7, set to 8 to make badger accept data larger than 1.1TB at the cost of larger compactions
		opts.MaxLevels = s.maxCompactionLevels
	}
	s.logger.Infof("Max Compaction Levels: %v", opts.MaxLevels)

	if s.blockCacheSize > 0 {
		opts.BlockCacheSize = s.blockCacheSize
	} else {
		opts.BlockCacheSize = int64(opts.BlockSize) * 1024 * 1024
	}

	// override default of 2GB (Int.Max)
	if s.valueLogFileSize > 0 {
		opts.ValueLogFileSize = s.valueLogFileSize
	}

	opts.MemTableSize = 128 * 1024 * 1024 // 128MB
	opts.DetectConflicts = false
	opts.NumVersionsToKeep = 1

	s.logger.Infof("setting BlockCacheSize: %v", opts.BlockCacheSize)
	opts.Logger = BadgerLogger{Logger: s.logger.Named("badger")} // override the default getLogger
	db, err := badger.Open(opts)
	if err != nil {
		s.logger.Error(err)
	}

	// if new storage, create unique storage id file. BackupManager can use this id to ensure it does not overwrite
	// a backup belonging to a different storage
	storageIDFile := filepath.Join(s.storeLocation, "DATAHUB_BACKUPID")
	if _, err = os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
		err = os.WriteFile(storageIDFile, []byte(fmt.Sprintf("%v", time.Now().UnixNano())), 0o644)
		if err != nil {
			s.logger.Error(err)
		}
	}

	s.logger.Info("database opened")
	s.database = db

	// get next internal id for dataset
	nextDatasetIDBytes := s.readValue(StoreNextDatasetIDBytes)
	if nextDatasetIDBytes == nil {
		s.nextDatasetID = 1
	} else {
		s.nextDatasetID = binary.BigEndian.Uint32(nextDatasetIDBytes)
	}

	err = s.loadDatasets()
	if err != nil {
		return err
	}

	// load namespace state
	nsState := &NamespacesState{}
	err = s.GetObject(NamespacesIndex, "namespacestate", nsState)
	if err != nil {
		return err
	}

	if nsState.ExpansionToPrefixMapping != nil {
		s.NamespaceManager.lock.Lock()
		s.NamespaceManager.expansionToPrefixMapping = nsState.ExpansionToPrefixMapping
		s.NamespaceManager.lock.Unlock()
	}

	if nsState.PrefixToExpansionMapping != nil {
		s.NamespaceManager.lock.Lock()
		s.NamespaceManager.prefixToExpansionMapping = nsState.PrefixToExpansionMapping
		s.NamespaceManager.lock.Unlock()
	}

	// load deleted datasets
	err = s.GetObject(StoreMetaIndex, "deleteddatasets", &s.deletedDatasets)
	if err != nil {
		return err
	}

	// initialise idseq
	key := []byte("uriids")
	numEntities := uint64(1000)
	s.idseq, _ = s.database.GetSequence(key, numEntities)

	// all good
	return nil
}

func (s *Store) Close() error {
	// release any unused ids
	s.idseq.Release()

	// close database
	return s.database.Close()
}

// loadDatasets from storage
func (s *Store) loadDatasets() error {
	// iterate keys starting with SYS_DATASETS_ID
	prefix := make([]byte, 2)
	binary.BigEndian.PutUint16(prefix, SysDatasetsID)
	return s.iterateObjects(prefix, reflect.TypeOf(Dataset{}), func(i interface{}) error {
		ds := i.(*Dataset)
		ds.store = s
		s.datasets.Store(ds.ID, ds)
		s.datasetsByInternalID.Store(ds.InternalID, ds)
		return nil
	})
}

func (s *Store) mergeInto(target *Entity, source *Entity) {
	for sk, sv := range source.Properties {
		if tv, ok := target.Properties[sk]; ok {
			// property already exists - add to existing list
			// or make a list
			switch v := tv.(type) {
			case []interface{}:
				switch svv := sv.(type) {
				case []interface{}:
					target.Properties[sk] = append(v, svv...)
				default:
					// else just add single value
					target.Properties[sk] = append(v, sv)
				}
			default:
				values := make([]interface{}, 0)
				values = append(values, v)
				// if source value is list add members
				switch svv := sv.(type) {
				case []interface{}:
					values = append(values, svv...)
				default:
					// else just add single value
					values = append(values, sv)
				}
				target.Properties[sk] = values
			}
		} else {
			// add new kv to target
			target.Properties[sk] = sv
		}
	}

	// and refs
	for sk, sv := range source.References {
		if tv, ok := target.References[sk]; ok {
			// property already exists - add to existing list
			// or make a list
			switch v := tv.(type) {
			case []interface{}:
				switch svv := sv.(type) {
				case []interface{}:
					target.References[sk] = append(v, svv...)
				default:
					// else just add single value
					target.References[sk] = append(v, sv)
				}
			default:
				values := make([]interface{}, 0)
				values = append(values, v)
				// if source value is list add members
				switch svv := sv.(type) {
				case []interface{}:
					values = append(values, svv...)
				default:
					// else just add single value
					values = append(values, sv)
				}
				target.References[sk] = values
			}
		} else {
			// add new kv to target
			target.References[sk] = sv
		}
	}
}

func (s *Store) mergePartials(partials []*Entity) *Entity {
	if len(partials) == 1 {
		return partials[0]
	} else if len(partials) > 1 {
		r := partials[0]
		for i := 1; i < len(partials); i++ {
			s.mergeInto(r, partials[i])
		}
		return r
	}
	return nil
}

func (s *Store) createMultiOriginEntity(partials []*Entity) *Entity {
	result := &Entity{}
	result.ID = partials[0].ID
	result.Properties = make(map[string]interface{})
	result.References = make(map[string]interface{})
	propKey := "http://data.mimiro.io/core/partials"
	result.Properties[propKey] = make([]interface{}, 0)

	for _, partial := range partials {
		result.Properties[propKey] = append(result.Properties[propKey].([]interface{}), partial)
	}

	return result
}

func (s *Store) IsCurie(uri string) bool {
	return strings.HasPrefix(uri, "ns")
}

func (s *Store) GetEntity(uri string, datasets []string, mergePartials bool) (*Entity, error) {
	var curie string
	var err error
	if strings.HasPrefix(uri, "ns") {
		curie = uri
	} else {
		curie, err = s.GetNamespacedIdentifierFromURI(uri)
		if err != nil {
			return nil, err
		}
	}

	rtxn := s.database.NewTransaction(false)
	defer rtxn.Discard()
	internalID, exists, err := s.getIDForURI(rtxn, curie)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil // todo: maybe an empty entity
	}
	scope := s.DatasetsToInternalIDs(datasets)
	entity, err := s.GetEntityWithInternalID(internalID, scope, mergePartials)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

func (s *Store) GetEntityAtPointInTimeWithInternalID(
	internalID uint64,
	at int64,
	targetDatasetIds []uint32,
	mergePartials bool,
) (*Entity, error) {
	/*
		binary.BigEndian.PutUint16(entityIdBuffer, ENTITY_ID_TO_JSON_INDEX_ID)
		binary.BigEndian.PutUint64(entityIdBuffer[2:], rid)
		binary.BigEndian.PutUint32(entityIdBuffer[10:], ds.InternalID)
		binary.BigEndian.PutUint64(entityIdBuffer[14:], uint64(txnTime))
		binary.BigEndian.PutUint16(entityIdBuffer[22:], uint16(jsonLength))
	*/

	// open read txn
	rtxn := s.database.NewTransaction(false)
	defer rtxn.Discard()

	entityLocatorPrefixBuffer := make([]byte, 10)
	binary.BigEndian.PutUint16(entityLocatorPrefixBuffer, EntityIDToJSONIndexID)
	binary.BigEndian.PutUint64(entityLocatorPrefixBuffer[2:], internalID)

	opts1 := badger.DefaultIteratorOptions
	// opts1.PrefetchSize = 1
	opts1.PrefetchValues = false
	opts1.Prefix = entityLocatorPrefixBuffer
	entityLocatorIterator := rtxn.NewIterator(opts1)
	defer entityLocatorIterator.Close()

	partials := make([]*Entity, 0) // there may be more one representation that is valid

	var prevValueBytes []byte
	var previousDatasetID uint32 = 0
	var currentDatasetID uint32 = 0
	var hasDeleted bool
	for entityLocatorIterator.Seek(entityLocatorPrefixBuffer); entityLocatorIterator.ValidForPrefix(entityLocatorPrefixBuffer); entityLocatorIterator.Next() {
		item := entityLocatorIterator.Item()
		key := item.Key()
		recordedTime := int64(binary.BigEndian.Uint64(key[14:]))

		if at < recordedTime {
			continue
		}
		currentDatasetID = binary.BigEndian.Uint32(key[10:])

		// check if dataset has been deleted, or must be excluded
		datasetDeleted := s.deletedDatasets[currentDatasetID]
		datasetIncluded := len(targetDatasetIds) == 0 // no specified datasets means no restriction - all datasets are allowed
		if !datasetIncluded {
			for _, id := range targetDatasetIds {
				if id == currentDatasetID {
					datasetIncluded = true
					break
				}
			}
		}
		if datasetDeleted || !datasetIncluded {
			continue
		}

		if previousDatasetID != 0 {
			if currentDatasetID != previousDatasetID {
				e := &Entity{}
				e.Properties = make(map[string]interface{})
				e.References = make(map[string]interface{})
				err := json.Unmarshal(prevValueBytes, e)
				if err != nil {
					return nil, err
				}
				if !e.IsDeleted {
					if !mergePartials {
						// add dataset to entity
						ds, ok := s.datasetsByInternalID.Load(previousDatasetID)
						if ok {
							e.Properties["http://data.mimiro.io/core/datasetname"] = (ds.(*Dataset)).ID
						} else {
							return nil, errors.New("dataset not found")
						}
					}
					partials = append(partials, e)
				} else {
					hasDeleted = true
				}
			}
		}

		previousDatasetID = currentDatasetID

		// fixme: pre alloc big ish buffer once and use value size
		prevValueBytes, _ = item.ValueCopy(nil)
	}

	if previousDatasetID != 0 {
		e := &Entity{}
		err := json.Unmarshal(prevValueBytes, e)
		if err != nil {
			return nil, err
		}
		if e.Properties == nil {
			e.Properties = make(map[string]interface{})
		}
		if e.References == nil {
			e.References = make(map[string]interface{})
		}
		if !e.IsDeleted {
			if !mergePartials {
				// add dataset to entity
				ds, ok := s.datasetsByInternalID.Load(previousDatasetID)
				if ok {
					e.Properties["http://data.mimiro.io/core/datasetname"] = (ds.(*Dataset)).ID
				} else {
					return nil, errors.New("dataset not found")
				}
			}

			partials = append(partials, e)
		} else {
			hasDeleted = true
		}
	}
	// merge partials
	var resultEntity *Entity
	if mergePartials {
		resultEntity = s.mergePartials(partials)
	} else if len(partials) > 0 {
		resultEntity = s.createMultiOriginEntity(partials)
	}

	// if no entity for this id then return the empty object
	if resultEntity == nil {
		resultEntity = &Entity{}
		resultEntity.Properties = make(map[string]interface{})
		resultEntity.References = make(map[string]interface{})

		resultEntity.InternalID = internalID
		resultEntity.IsDeleted = hasDeleted
		uri, err := s.getURIForID(internalID)
		if err != nil {
			return nil, err
		}
		resultEntity.ID = uri
	}

	return resultEntity, nil
}

func (s *Store) GetEntityWithInternalID(internalID uint64, targetDatasetIds []uint32, mergePartials bool) (*Entity, error) {
	return s.GetEntityAtPointInTimeWithInternalID(internalID, time.Now().UnixNano(), targetDatasetIds, mergePartials)
}

type RelatedEntityResult struct {
	StartURI      string
	PredicateURI  string
	RelatedEntity *Entity
}
type RelatedEntitiesResult struct {
	Continuation *RelatedFrom
	Relations    []RelatedEntityResult
}

type RelatedFrom struct {
	RelationIndexFromKey []byte
	Predicate            uint64
	Inverse              bool
	Datasets             []uint32
	At                   int64
}
type RelatedEntitiesQueryResult struct {
	Cont      []*RelatedFrom
	Relations []RelatedEntityResult
}

// Backwards compatibility function
func (s *Store) GetManyRelatedEntities(
	startPoints []string,
	predicate string,
	inverse bool,
	datasets []string,
	mergePartials bool) ([][]any, error) {
	res, err := s.GetManyRelatedEntitiesBatch(startPoints, predicate, inverse, datasets, 0, mergePartials)
	if err != nil {
		return nil, err
	}

	legacyResult := ToLegacyQueryResult(res)
	return legacyResult, nil
}

func ToLegacyQueryResult(res RelatedEntitiesQueryResult) [][]any {
	legacyResult := make([][]any, len(res.Relations))
	for k, r := range res.Relations {
		item := make([]any, 3)
		item[0] = r.StartURI
		item[1] = r.PredicateURI
		item[2] = r.RelatedEntity
		legacyResult[k] = item
	}
	return legacyResult
}

func (s *Store) GetManyRelatedEntitiesBatch(
	startPoints []string,
	predicate string,
	inverse bool,
	datasets []string,
	limit int, mergePartials bool) (RelatedEntitiesQueryResult, error) {
	queryTime := time.Now().UnixNano()
	from, err := s.ToRelatedFrom(startPoints, predicate, inverse, datasets, queryTime)
	if err != nil {
		return RelatedEntitiesQueryResult{}, err
	}
	return s.GetManyRelatedEntitiesAtTime(from, limit, mergePartials)
}

func (s *Store) ToRelatedFrom(
	startPoints []string,
	predicate string,
	inverse bool,
	datasets []string,
	queryTime int64,
) ([]*RelatedFrom, error) {
	targetDatasetIds := s.DatasetsToInternalIDs(datasets)
	from := make([]*RelatedFrom, len(startPoints))
	var resourceCurie string
	var err error
	txn := s.database.NewTransaction(false)
	defer txn.Discard()

	pid, err := s.GetPredicateID(predicate, txn)
	if err != nil {
		return nil, err
	}
	for i, uri := range startPoints {
		if strings.HasPrefix(uri, "ns") {
			resourceCurie = uri
		} else {
			resourceCurie, err = s.GetNamespacedIdentifierFromURI(uri)
			if err != nil {
				return nil, err
			}
		}
		rid, ridExists, err := s.getIDForURI(txn, resourceCurie)
		if err != nil {
			return nil, err
		}
		if !ridExists {
			return nil, err
		}

		// define search prefix
		searchBuffer := make([]byte, 10)
		if inverse {
			binary.BigEndian.PutUint16(searchBuffer, IncomingRefIndex)
		} else {
			binary.BigEndian.PutUint16(searchBuffer, OutgoingRefIndex)
		}
		binary.BigEndian.PutUint64(searchBuffer[2:], rid)
		from[i] = &RelatedFrom{
			RelationIndexFromKey: searchBuffer,
			Predicate:            pid,
			Inverse:              inverse,
			Datasets:             targetDatasetIds,
			At:                   queryTime,
		}
	}
	return from, nil
}

func (s *Store) GetPredicateID(predicate string, txn *badger.Txn) (uint64, error) {
	if txn == nil {
		txn = s.database.NewTransaction(false)
		defer txn.Discard()
	}
	var predCurie string
	var err error
	if predicate != "*" {
		if strings.HasPrefix(predicate, "ns") {
			predCurie = predicate
		} else {
			predCurie, err = s.GetNamespacedIdentifierFromURI(predicate)
			if err != nil {
				return 0, err
			}
		}
	}
	var pid uint64
	if predicate != "*" {
		var pidExists bool
		pid, pidExists, err = s.getIDForURI(txn, predCurie)
		if err != nil {
			return 0, err
		}

		if !pidExists {
			return 0, fmt.Errorf("could not load predicate id for %v", predicate)
		}
	}
	return pid, err
}

func (s *Store) GetManyRelatedEntitiesAtTime(from []*RelatedFrom, limit int, mergePartials bool) (RelatedEntitiesQueryResult, error) {
	result := RelatedEntitiesQueryResult{}
	unlimited := limit == 0
	var relatedFroms []*RelatedFrom
	for _, startPoint := range from {
		if (limit > 0) || unlimited {
			relatedEntities, err := s.getRelatedEntitiesAtTime(startPoint, limit, mergePartials)
			if err != nil {
				return RelatedEntitiesQueryResult{}, err
			}
			if relatedEntities.Continuation != nil {
				relatedFroms = append(relatedFroms, relatedEntities.Continuation)
			}
			result.Relations = append(result.Relations, relatedEntities.Relations...)
			limit = int(math.Max(float64(limit-len(relatedEntities.Relations)), 0))
		} else {
			relatedFroms = append(relatedFroms, startPoint)
		}
	}
	result.Cont = relatedFroms
	return result, nil
}

func (s *Store) getRelatedEntitiesAtTime(from *RelatedFrom, limit int, mergePartials bool) (RelatedEntitiesResult, error) {
	relations, cont, err := s.GetRelatedAtTime(from, limit)
	if err != nil {
		return RelatedEntitiesResult{}, err
	}
	result := make([]RelatedEntityResult, len(relations))
	startURI, err := s.getURIForID(binary.BigEndian.Uint64(from.RelationIndexFromKey[2:10]))
	if err != nil {
		return RelatedEntitiesResult{}, err
	}
	for i, r := range relations {
		predicateURI, err := s.getURIForID(r.PredicateID)
		if err != nil {
			return RelatedEntitiesResult{}, err
		}

		relatedEntity, err := s.GetEntityWithInternalID(r.EntityID, from.Datasets, mergePartials)
		if err != nil {
			return RelatedEntitiesResult{}, err
		}

		r := RelatedEntityResult{
			StartURI:      startURI,
			PredicateURI:  predicateURI,
			RelatedEntity: relatedEntity,
		}

		result[i] = r
	}
	return RelatedEntitiesResult{Relations: result, Continuation: cont}, nil
}

// DatasetsToInternalIDs map dataset IDs (strings) to InternaIDs (uint32)
func (s *Store) DatasetsToInternalIDs(datasets []string) []uint32 {
	var scopeArray []uint32
	if len(datasets) > 0 {
		for _, ds := range datasets {
			dataset, found := s.datasets.Load(ds)
			if found {
				scopeArray = append(scopeArray, dataset.(*Dataset).InternalID)
			}
		}
	}
	return scopeArray
}

// unit test only
func (s *Store) getRelated(
	startPoint string,
	predicate string,
	inverse bool,
	datasets []string, mergePartials bool) ([]RelatedEntityResult, error) {
	res, err := s.GetManyRelatedEntitiesBatch([]string{startPoint}, predicate, inverse, datasets, 0, mergePartials)
	return res.Relations, err
}

func (s *Store) GetRelatedAtTime(from *RelatedFrom, limit int) ([]qresult, *RelatedFrom, error) {
	results := make([]qresult, 0)
	if from == nil || len(from.RelationIndexFromKey) < 10 {
		return nil, nil, fmt.Errorf("invalid query startpoint: %+v", from)
	}
	// make a copy of *RelatedFrom
	cont := *from
	cont.RelationIndexFromKey = make([]byte, 40)
	// lookup pred and id
	err := s.database.View(func(txn *badger.Txn) error {
		if from.Inverse {

			searchBuffer := from.RelationIndexFromKey[:10] // copy by value
			startBuffer := from.RelationIndexFromKey
			// isFirstPage := len(from.RelationIndexFromKey) == 10

			// skipToPageStart := !isFirstPage

			opts1 := badger.DefaultIteratorOptions
			opts1.PrefetchValues = false
			opts1.Prefix = searchBuffer
			// opts1.Reverse = true
			outgoingIterator := txn.NewIterator(opts1)
			defer outgoingIterator.Close()

			var currentRID uint64
			currentRID = 0
			// tmpResult := make(map[[12]byte]result)

			var prevResults map[uint64]qresult = map[uint64]qresult{}
			var prevDeleted bool
			var prevDatasetID uint32
			var dsSpillOver map[uint32]qresult
			for outgoingIterator.Seek(startBuffer); outgoingIterator.ValidForPrefix(searchBuffer); outgoingIterator.Next() {
				//if skipToPageStart {
				//	skipToPageStart = false
				//	outgoingIterator.Next() // NextIdxKey is actually the last key of previous page. forward once to next item
				//	if !outgoingIterator.ValidForPrefix(searchBuffer[:10]) {
				//		break
				//	}
				//}
				if limit != 0 && len(results) >= limit {
					break
				}
				item := outgoingIterator.Item()
				k := item.Key()

				// check dataset if deleted, or if excluded from search
				datasetID := binary.BigEndian.Uint32(k[36:])

				// no specified datasets means no restriction - all datasets are allowed
				datasetIncluded := len(from.Datasets) == 0
				if !datasetIncluded {
					for _, id := range from.Datasets {
						if id == datasetID {
							datasetIncluded = true
							break
						}
					}
				}

				if s.deletedDatasets[datasetID] || !datasetIncluded {
					continue
				}

				// get recorded time on relationship
				// if rel recorded time gt than query time then continue
				et := int64(binary.BigEndian.Uint64(k[18:]))
				if et > from.At {
					continue
				}

				// get related
				relatedID := binary.BigEndian.Uint64(k[10:])

				// get Predicate
				predID := binary.BigEndian.Uint64(k[26:])
				if from.Predicate != predID && from.Predicate > 0 {
					continue
				}

				// since we are going forward through the index, we know the previous
				// result is the most current result for this related entity. so we add it to the results
				if relatedID != currentRID {
					// add to results if not deleted
					if currentRID != 0 && !prevDeleted {
						for _, prevResult := range prevResults {
							results = append(results, prevResult)
						}
					} else {
						// if the last result was deleted, check if there
						// are any non deleted results for other datasets containing this relation.
						// dsSpillOver only contains a non-deleted result for a dataset or nothing
						for _, dsResult := range dsSpillOver {
							results = append(results, dsResult)
							break
						}
					}
					// reset dsSpillOver since we start a new related entity now
					dsSpillOver = map[uint32]qresult{}
					prevResults = map[uint64]qresult{}
				} else if datasetID != prevDatasetID {
					// if the related entity is the same, but the dataset is different,
					// we want to track the most current version of the related entity
					// for each dataset. non-deleted only.
					// this is needed so we can detect if *any* dataset contains a non-deleted
					// relation, because then we want to include it in the results.
					if prevDeleted {
						delete(dsSpillOver, prevDatasetID)
					}
					if currentRID != 0 && !prevDeleted {
						for _, prevResult := range prevResults {
							if prevResult.DatasetID == prevDatasetID {

								dsSpillOver[prevDatasetID] = prevResult
							}
						}
					}
				}

				del := binary.BigEndian.Uint16(k[34:])
				prevDeleted = del == 1
				prevDatasetID = datasetID
				prevResults[predID] = qresult{Time: uint64(et), EntityID: relatedID, PredicateID: predID, DatasetID: datasetID}

				// set current to be this related object
				currentRID = relatedID

				copy(cont.RelationIndexFromKey, k)
			}
			var added bool
			// add the last iteration result
			if limit == 0 || len(results) < limit {
				if currentRID != 0 && !prevDeleted {
					for _, prevResult := range prevResults {
						results = append(results, prevResult)
					}
					added = true
				} else if dsSpillOver != nil && (limit == 0 || len(results) < limit) {
					// if there is a spillOver left, add it
					if prevDeleted {
						delete(dsSpillOver, prevDatasetID)
					}
					for _, dsResult := range dsSpillOver {
						results = append(results, dsResult)
						added = true
						break
					}
				}
			}
			// mark this as the last query result page
			if !outgoingIterator.ValidForPrefix(searchBuffer) && (len(results) == 0 || added) {
				cont.RelationIndexFromKey = nil
			}
		} else {
			/*  binary.BigEndian.PutUint16(outgoingBuffer, OUTGOING_REF_INDEX)
			binary.BigEndian.PutUint64(outgoingBuffer[2:], rid)
			binary.BigEndian.PutUint64(outgoingBuffer[10:], uint64(txnTime))
			binary.BigEndian.PutUint64(outgoingBuffer[18:], predid)
			binary.BigEndian.PutUint64(outgoingBuffer[26:], relatedid)
			binary.BigEndian.PutUint16(outgoingBuffer[34:], 0) // deleted.
			binary.BigEndian.PutUint32(outgoingBuffer[36:], ds.InternalID) */
			// searchBuffer := make([]byte, 10)
			// binary.BigEndian.PutUint16(searchBuffer, OUTGOING_REF_INDEX)
			// binary.BigEndian.PutUint64(searchBuffer[2:], rid)

			searchBuffer := from.RelationIndexFromKey[:10] // copy by value
			reverseFrom := make([]byte, 11)
			copy(reverseFrom, searchBuffer)
			reverseFrom[10] = 0xFF
			opts1 := badger.DefaultIteratorOptions
			opts1.PrefetchValues = false
			opts1.Prefix = searchBuffer
			opts1.Reverse = true
			outgoingIterator := txn.NewIterator(opts1)
			defer outgoingIterator.Close()

			seenIds := map[uint64]map[uint64]map[uint32]bool{}
			added := map[uint64]map[uint64]bool{}
			var hasReachedStartKey bool
			if len(from.RelationIndexFromKey) == 10 {
				hasReachedStartKey = true
			}
			for outgoingIterator.Seek(reverseFrom); outgoingIterator.ValidForPrefix(searchBuffer); outgoingIterator.Next() {
				item := outgoingIterator.Item()
				k := item.Key()

				datasetID := binary.BigEndian.Uint32(k[36:])

				// no specified datasets means no restriction - all datasets are allowed
				datasetIncluded := len(from.Datasets) == 0
				if !datasetIncluded {
					for _, id := range from.Datasets {
						if id == datasetID {
							datasetIncluded = true
							break
						}
					}
				}

				if s.deletedDatasets[datasetID] || !datasetIncluded {
					continue
				}

				// get recorded time on relationship
				// if rel recorded time gt than query time then continue
				et := int64(binary.BigEndian.Uint64(k[10:]))
				if et > from.At {
					continue
				}

				// get Predicate
				predID := binary.BigEndian.Uint64(k[18:])
				if from.Predicate != predID && from.Predicate > 0 {
					continue
				}
				// get related
				relatedID := binary.BigEndian.Uint64(k[26:])

				// init tracking maps for this predicate
				if seenIds[predID] == nil {
					seenIds[predID] = map[uint64]map[uint32]bool{}
					added[predID] = map[uint64]bool{}
				}

				// check if this related entity has been added to results
				isAdded := added[predID][relatedID]

				// init tracking map per dataset for this related entity
				// the existence of this map means we've seen this related entity for this predicate
				if seenIds[predID][relatedID] == nil {
					seenIds[predID][relatedID] = map[uint32]bool{}
				}
				// check if we've seen this related entity for this predicate for this dataset
				_, dsSeen := seenIds[predID][relatedID][datasetID]

				// since we're iterating in reverse, we only need to
				// consider the first instance of a related entity for a predicate + dataset combo
				// also, once we've added a related entity for a predicate to results,
				// we don't need to add it again
				if dsSeen || isAdded {
					continue
				}
				seenIds[predID][relatedID][datasetID] = true

				// get deleted
				del := binary.BigEndian.Uint16(k[34:])
				if del != 1 && hasReachedStartKey {
					if limit != 0 && len(results) >= limit {
						break
					}
					copy(cont.RelationIndexFromKey, k)
					results = append(results, qresult{Time: uint64(et), EntityID: relatedID, PredicateID: predID, DatasetID: datasetID})
					added[predID][relatedID] = true
				}

				// set at end of iteration so that we jump over the item the previous page gave as continuation, while still
				// adding it to seenIds
				if !hasReachedStartKey {
					if bytes.Equal(k[:40], from.RelationIndexFromKey) {
						hasReachedStartKey = true
					}
				}

			}
			if !outgoingIterator.ValidForPrefix(searchBuffer) {
				cont.RelationIndexFromKey = nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	//if len(results) == 0 {
	//	cont.RelationIndexFromKey = nil
	//}
	if cont.RelationIndexFromKey == nil {
		return results, nil, nil
	}
	return results, &cont, nil
}

func (s *Store) getIDForURI(txn *badger.Txn, uri string) (uint64, bool, error) {
	var rid uint64
	var exists bool

	if uri == "" {
		return 0, false, errors.New("URI cannot be empty")
	}

	// check if it exists already uri => id
	uriAsBytes := []byte(uri)
	uribuf := make([]byte, len(uriAsBytes)+2)
	binary.BigEndian.PutUint16(uribuf, URIToIDIndexID)
	copy(uribuf[2:], uriAsBytes)

	item, err := txn.Get(uribuf)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			rid = 0
			exists = false
		} else {
			s.logger.Error(err.Error())
			panic("error with get key")
		}
	} else {
		err := item.Value(func(val []byte) error {
			rid = binary.BigEndian.Uint64(val)
			exists = true
			return nil
		})
		if err != nil {
			return 0, false, err
		}
	}

	return rid, exists, nil
}

func (s *Store) commitIDTxn() error {
	s.idmux.Lock()
	defer s.idmux.Unlock()

	if s.idtxn == nil {
		// nothing to commit
		return nil
	}

	err := s.idtxn.Commit()
	if err != nil {
		return err
	}
	s.idtxn = nil
	return nil
}

func (s *Store) getURIForID(rid uint64) (string, error) {
	buf := make([]byte, 10) // seq id int 64 and 2 byte index id
	binary.BigEndian.PutUint16(buf, IDToURIIndexID)
	binary.BigEndian.PutUint64(buf[2:], rid)

	txn := s.database.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(buf)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return "", nil
		} else {
			s.logger.Error(err.Error())
			panic("error with get key")
		}
	} else {
		result := ""
		err := item.Value(func(val []byte) error {
			result = string(val)
			return nil
		})

		return result, err
	}
}

func (s *Store) assertIDForURI(uri string, localTxnCache map[string]uint64) (uint64, bool, error) {
	var rid uint64
	var exists bool
	isnew := false

	if uri == "" {
		return 0, false, errors.New("URI cannot be empty")
	}

	rid, exists = localTxnCache[uri]
	if exists {
		return rid, false, nil
	}

	// add lock
	s.idmux.Lock()
	defer s.idmux.Unlock()

	if s.idtxn == nil {
		s.idtxn = s.database.NewTransaction(true)
	}

	// check if it exists already uri => id
	uriAsBytes := []byte(uri)
	uribuf := make([]byte, len(uriAsBytes)+2)
	binary.BigEndian.PutUint16(uribuf, URIToIDIndexID)
	copy(uribuf[2:], uriAsBytes)

	item, err := s.idtxn.Get(uribuf)
	if err != nil {
		if err == badger.ErrKeyNotFound {

			seqbuf := make([]byte, 8)
			rid, _ = s.idseq.Next()
			binary.BigEndian.PutUint64(seqbuf, rid)

			err := s.idtxn.Set(uribuf, seqbuf)
			if err != nil {
				return 0, false, err
			}

			// inverse id to uri
			invuribuf := make([]byte, len(uriAsBytes))
			copy(invuribuf, uriAsBytes)

			invseqbuf := make([]byte, 10) // seq id int 64 and 2 byte index id
			binary.BigEndian.PutUint16(invseqbuf, IDToURIIndexID)
			binary.BigEndian.PutUint64(invseqbuf[2:], rid)

			err = s.idtxn.Set(invseqbuf, invuribuf)
			if err != nil {
				return 0, false, err
			}

			isnew = true
		} else {
			panic("error with get key")
		}
	} else {
		err := item.Value(func(val []byte) error {
			rid = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return 0, false, err
		}
	}

	// localTxnCache value
	localTxnCache[uri] = rid

	return rid, isnew, nil
}

func (s *Store) storeValue(key []byte, value []byte) error {
	tags := []string{
		"application:datahub",
	}
	err := s.database.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		_ = s.statsdClient.Count("store.added.bytes", int64(len(value)), tags, 1) // don't care about errors here
		return err
	})
	return err
}

func (s *Store) deleteValue(key []byte) error {
	err := s.database.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})
	return err
}

func (s *Store) moveValue(oldKey, newKey, newValue []byte) error {
	tags := []string{
		"application:datahub",
	}
	err := s.database.Update(func(txn *badger.Txn) error {
		err := txn.Delete(oldKey)
		if err != nil {
			return err
		}
		err = txn.Set(newKey, newValue)
		_ = s.statsdClient.Count("store.added.bytes", int64(len(newValue)), tags, 1) // don't care about errors here
		return err
	})
	return err
}

func (s *Store) readValue(key []byte) []byte {
	var val []byte

	err := s.database.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}

		if err != nil {
			panic(err.Error())
			// s.logger.Error(err)
			// return nil
		}

		val, _ = item.ValueCopy(nil)
		return nil
	})
	if err != nil {
		panic(err.Error())
	}

	tags := []string{
		"application:datahub",
	}

	if len(val) > 0 {
		s.statsdClient.Count("store.read.bytes", int64(len(val)), tags, 1) // don't care about errors here
	}

	return val
}

func (s *Store) StoreObject(collection CollectionIndex, id string, data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		s.logger.Error(err)
		return err
	}
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(collection))
	key := append(indexBytes, []byte("::"+id)...)
	err = s.storeValue(key, b)
	if err != nil {
		s.logger.Error(err)
		return err
	}

	return nil
}

func (s *Store) GetObject(collection CollectionIndex, id string, obj interface{}) error {
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(collection))
	key := append(indexBytes, []byte("::"+id)...)
	data := s.readValue(key)
	if len(data) == 0 {
		obj = nil
		return nil
	}

	err := json.Unmarshal(data, obj)
	if err != nil {
		s.logger.Errorw("GetObject error: ", err.Error())
		obj = nil
		return err
	}

	return nil
}

// DeleteObject is a slightly less to the metal variation of the deleteValue
// method. You should probably use this instead of deleteValue if you need
// to delete stuff. It takes a collection and an object id, and attempts to
// delete it from the store.
func (s *Store) DeleteObject(collection CollectionIndex, id string) error {
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(collection))
	key := append(indexBytes, []byte("::"+id)...)
	return s.deleteValue(key)
}

func (s *Store) iterateObjects(prefix []byte, t reflect.Type, visitFunc func(interface{}) error) error {
	err := s.database.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				o := reflect.New(t)
				i := o.Interface()
				err := json.Unmarshal(v, i)
				if err != nil {
					return err
				}
				return visitFunc(i)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *Store) IterateObjectsRaw(prefix []byte, processJSON func([]byte) error) error {
	err := s.database.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				err := processJSON(v)
				return err
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

type Transaction struct {
	DatasetEntities map[string][]*Entity
}

func (s *Store) ExecuteTransaction(transaction *Transaction) error {
	datasets := make(map[string]*Dataset)

	for k := range transaction.DatasetEntities {
		dataset, ok := s.datasets.Load(k)
		if !ok {
			return errors.New("no dataset " + k)
		}
		datasets[k] = dataset.(*Dataset)

		dataset.(*Dataset).WriteLock.Lock()
		// release lock at end regardless
		defer dataset.(*Dataset).WriteLock.Unlock()
	}

	txnTime := time.Now().UnixNano()
	txn := s.database.NewTransaction(true)
	defer txn.Discard()

	updateCountsPerDataset := make(map[string]int64)

	for k, ds := range datasets {
		entities := transaction.DatasetEntities[k]
		newItems, err := ds.StoreEntitiesWithTransaction(entities, txnTime, txn)
		if err != nil {
			return err
		}

		updateCountsPerDataset[k] = newItems
	}

	err := s.commitIDTxn()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	// update the txn counts
	for k, v := range updateCountsPerDataset {
		ds, ok := s.datasets.Load(k)
		if !ok {
			return errors.New("no dataset " + k)
		}

		err = ds.(*Dataset).updateDataset(v, nil)
		if err != nil {
			return err
		}
	}

	return nil
}
