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
	"context"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/dgraph-io/badger/v3"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"encoding/json"
)

type result struct {
	predicateID uint64
	entityID    uint64
	time        uint64
	datasetID   uint32
}

// Store data structure
type Store struct {
	database             *badger.DB
	datasets             *sync.Map              // concurrent map of datasets
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
}

type BadgerLogger struct { // we use this to implement the Badger Logger interface
	Logger *zap.SugaredLogger
}

func (bl BadgerLogger) Errorf(format string, v ...interface{})   { bl.Logger.Errorf(format, v...) }
func (bl BadgerLogger) Infof(format string, v ...interface{})    { bl.Logger.Infof(format, v...) }
func (bl BadgerLogger) Warningf(format string, v ...interface{}) { bl.Logger.Warnf(format, v...) }
func (bl BadgerLogger) Debugf(format string, v ...interface{})   { bl.Logger.Debugf(format, v...) }

// NewStore Create a new store
func NewStore(lc fx.Lifecycle, env *conf.Env, statsdClient statsd.ClientInterface) *Store {
	fsTimeout := env.FullsyncLeaseTimeout
	if fsTimeout == 0*time.Second {
		env.Logger.Warnf("No fullsync lease timeout set, fallback to 1h")
		fsTimeout = 1 * time.Hour
	}
	store := &Store{
		datasets:             &sync.Map{},
		deletedDatasets:      make(map[uint32]bool),
		storeLocation:        env.StoreLocation,
		logger:               env.Logger.Named("store"),
		statsdClient:         statsdClient,
		fullsyncLeaseTimeout: fsTimeout,
		blockCacheSize:       env.BlockCacheSize,
	}
	store.NamespaceManager = NewNamespaceManager(store)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			store.logger.Infof("Opening store in location: %s", store.storeLocation)
			return store.Open()
		},
		OnStop: func(ctx context.Context) error {
			store.logger.Infof("Unmounting store")
			return store.Close()
		},
	})

	return store
}

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
	Id         string            `json:"id"`
	Namespaces map[string]string `json:"namespaces"`
}

// GetContext return a context instance
//    containing namespace mappings for the given list of namespace prefixes
//    if the give list of given namespace prefixes is empty, all namespace mappings are returned
func (namespaceManager *NamespaceManager) GetContext(includedNamespaces []string) (context *Context) {
	if len(includedNamespaces) > 0 {
		filteredPrefixMapping := map[string]string{}
		for _, expansionURI := range includedNamespaces {
			prefix, _ := namespaceManager.GetPrefixMappingForExpansion(expansionURI)
			filteredPrefixMapping[prefix] = expansionURI
		}
		context = &Context{Id: "@context", Namespaces: filteredPrefixMapping}
	} else {
		context = &Context{Id: "@context", Namespaces: namespaceManager.GetPrefixToExpansionMap()}
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
		err := namespaceManager.store.StoreObject(NAMESPACES_INDEX, "namespacestate", state)
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

	return DsNsInfo{DatasetPrefix: prefix, PublicNamespacesKey: prefix + ":publicNamespaces",
		NameKey: prefix + ":name", ItemsKey: prefix + ":items"}, nil
}

func getUrlParts(url string) (string, string, error) {

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

func (s *Store) GetNamespacedIdentifierFromUri(val string) (string, error) {
	if strings.HasPrefix(val, "http://") || strings.HasPrefix(val, "https://") {
		expansion, lastPathPart, err := getUrlParts(val)
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
		expansion, lastPathPart, err := getUrlParts(val)
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
		expansion, lastPathPart, err := getUrlParts(val)
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

func (s *Store) GetGlobalContext() *Context {
	return s.NamespaceManager.GetContext(nil)
}

// Open Opens the store. Must be called before using.
func (s *Store) Open() error {
	s.logger.Info("Open database")
	opts := badger.DefaultOptions(s.storeLocation)

	if s.blockCacheSize > 0 {
		opts.BlockCacheSize = s.blockCacheSize
	} else {
		opts.BlockCacheSize = int64(opts.BlockSize) * 1024 * 1024
	}
	opts.MemTableSize = 128 * 1024 * 1024 //128MB

	s.logger.Infof("setting BlockCacheSize: %v", opts.BlockCacheSize)
	opts.Logger = BadgerLogger{Logger: s.logger.Named("badger")} // override the default getLogger
	db, err := badger.Open(opts)
	if err != nil {
		s.logger.Error(err)
	}

	s.logger.Info("database opened")
	s.database = db

	// get next internal id for dataset
	nextDatasetIDBytes := s.readValue(STORE_NEXT_DATASET_ID_BYTES)
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
	err = s.GetObject(NAMESPACES_INDEX, "namespacestate", nsState)
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
	err = s.GetObject(STORE_META_INDEX, "deleteddatasets", &s.deletedDatasets)
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
	binary.BigEndian.PutUint16(prefix, SYS_DATASETS_ID)
	return s.iterateObjects(prefix, reflect.TypeOf(Dataset{}), func(i interface{}) error {
		ds := i.(*Dataset)
		ds.store = s
		s.datasets.Store(ds.ID, ds)
		return nil
	})
}

func (s *Store) MergeInto(target *Entity, source *Entity) {
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

func (s *Store) MergePartials(partials []*Entity) *Entity {
	if len(partials) == 1 {
		return partials[0]
	} else if len(partials) > 1 {
		r := partials[0]
		for i := 1; i < len(partials); i++ {
			s.MergeInto(r, partials[i])
		}
		return r
	}
	return nil
}

func (s *Store) IsCurie(uri string) bool {
	return strings.HasPrefix(uri, "ns")
}

func (s *Store) GetEntity(uri string, datasets []string) (*Entity, error) {
	var curie string
	var err error
	if strings.HasPrefix(uri, "ns") {
		curie = uri
	} else {
		curie, err = s.GetNamespacedIdentifierFromUri(uri)
		if err != nil {
			return nil, err
		}
	}

	rtxn := s.database.NewTransaction(false)
	defer rtxn.Discard()
	internalId, exists, err := s.getIDForURI(rtxn, curie)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil // todo: maybe an empty entity
	}
	scope := s.datasetsToInternalIDs(datasets)
	entity, err := s.GetEntityWithInternalId(internalId, scope)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

func (s *Store) GetEntityAtPointInTime(uri string, at int64) *Entity {
	return nil
}

func (s *Store) GetEntityAtPointInTimeWithInternalID(internalId uint64, at int64, targetDatasetIds []uint32) (*Entity, error) {
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

	opts1 := badger.DefaultIteratorOptions
	// opts1.PrefetchValues = false
	entityLocatorIterator := rtxn.NewIterator(opts1)
	defer entityLocatorIterator.Close()

	entityLocatorPrefixBuffer := make([]byte, 10)
	binary.BigEndian.PutUint16(entityLocatorPrefixBuffer, ENTITY_ID_TO_JSON_INDEX_ID)
	binary.BigEndian.PutUint64(entityLocatorPrefixBuffer[2:], internalId)

	partials := make([]*Entity, 0) // there may be more one representation that is valid

	var prevValueBytes []byte
	var previousDatasetId uint32 = 0
	var currentDatasetId uint32 = 0
	for entityLocatorIterator.Seek(entityLocatorPrefixBuffer); entityLocatorIterator.ValidForPrefix(entityLocatorPrefixBuffer); entityLocatorIterator.Next() {
		item := entityLocatorIterator.Item()
		key := item.Key()
		recordedTime := int64(binary.BigEndian.Uint64(key[14:]))

		if at < recordedTime {
			continue
		}

		currentDatasetId = binary.BigEndian.Uint32(key[10:])

		// check if dataset has been deleted, or must be excluded
		datasetDeleted := s.deletedDatasets[currentDatasetId]
		datasetIncluded := len(targetDatasetIds) == 0 //no specified datasets means no restriction - all datasets are allowed
		if !datasetIncluded {
			for _, id := range targetDatasetIds {
				if id == currentDatasetId {
					datasetIncluded = true
					break
				}
			}
		}
		if datasetDeleted || !datasetIncluded {
			continue
		}

		if previousDatasetId != 0 {
			if currentDatasetId != previousDatasetId {
				e := &Entity{}
				e.Properties = make(map[string]interface{})
				e.References = make(map[string]interface{})
				err := json.Unmarshal(prevValueBytes, e)
				if err != nil {
					return nil, err
				}
				partials = append(partials, e)
			}
		}

		previousDatasetId = currentDatasetId

		// fixme: pre alloc big ish buffer once and use value size
		prevValueBytes, _ = item.ValueCopy(nil)
	}

	if previousDatasetId != 0 {
		e := &Entity{}
		e.Properties = make(map[string]interface{})
		e.References = make(map[string]interface{})
		err := json.Unmarshal(prevValueBytes, e)
		if err != nil {
			return nil, err
		}
		partials = append(partials, e)
	}

	// merge partials
	mergedEntity := s.MergePartials(partials)

	// if no entity for this id then return the empty object
	if mergedEntity == nil {
		mergedEntity = &Entity{}
		mergedEntity.Properties = make(map[string]interface{})
		mergedEntity.References = make(map[string]interface{})

		mergedEntity.InternalID = internalId
		uri, err := s.getURIForID(internalId)
		if err != nil {
			return nil, err
		}
		mergedEntity.ID = uri
	}

	return mergedEntity, nil
}

func (s *Store) GetEntityWithInternalId(internalId uint64, targetDatasetIds []uint32) (*Entity, error) {
	return s.GetEntityAtPointInTimeWithInternalID(internalId, time.Now().UnixNano(), targetDatasetIds)
}

type QueryResult struct {
	ColumnNames       []string
	Rows              [][]interface{}
	ContinuationToken string
}

// type RelatedEntitiesQueryResult [][]interface{}

func (s *Store) GetManyRelatedEntities(startFromUris []string, predicate string, inverse bool, datasets []string) ([][]interface{}, error) {
	queryTime := time.Now().UnixNano()
	return s.GetManyRelatedEntitiesAtTime(startFromUris, predicate, inverse, datasets, queryTime)
}

func (s *Store) GetManyRelatedEntitiesAtTime(startFromUris []string, predicate string, inverse bool, datasets []string, at int64) ([][]interface{}, error) {
	result := make([][]interface{}, 0)
	for _, uri := range startFromUris {
		relatedEntities, err := s.GetRelatedEntitiesAtTime(uri, predicate, inverse, datasets, at)
		if err != nil {
			return nil, err
		}

		result = append(result, relatedEntities...)
	}
	return result, nil
}

func (s *Store) GetRelatedEntitiesAtTime(uri string, predicate string, inverse bool, datasets []string, at int64) ([][]interface{}, error) {
	targetDatasetIds := s.datasetsToInternalIDs(datasets)

	results, err := s.GetRelatedAtTime(uri, predicate, inverse, targetDatasetIds, at)
	if err != nil {
		return nil, err
	}
	result := make([][]interface{}, len(results))
	for i, r := range results {
		predicateURI, err := s.getURIForID(r.predicateID)
		if err != nil {
			return nil, err
		}

		relatedEntity, err := s.GetEntityWithInternalId(r.entityID, targetDatasetIds)
		if err != nil {
			return nil, err
		}

		r := make([]interface{}, 3)
		r[0] = uri
		r[1] = predicateURI
		r[2] = relatedEntity

		result[i] = r
	}
	return result, nil
}

//datasetsToInternalIDs map dataset IDs (strings) to InternaIDs (uint32)
func (s *Store) datasetsToInternalIDs(datasets []string) []uint32 {
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

func (s *Store) GetRelatedEntities(uri string, predicate string, inverse bool, datasets []string) ([][]interface{}, error) {
	queryTime := time.Now().UnixNano()
	return s.GetRelatedEntitiesAtTime(uri, predicate, inverse, datasets, queryTime)
}

func (s *Store) GetRelated(uri string, predicate string, inverse bool, datasets []string) ([]result, error) {
	queryTime := time.Now().UnixNano()
	targetDatasetIds := s.datasetsToInternalIDs(datasets)
	return s.GetRelatedAtTime(uri, predicate, inverse, targetDatasetIds, queryTime)
}

func (s *Store) GetRelatedAtTime(uri string, predicate string, inverse bool, targetDatasetIds []uint32, queryTime int64) ([]result, error) {
	var resourceCurie, predCurie string
	var err error

	if strings.HasPrefix(uri, "ns") {
		resourceCurie = uri
	} else {
		resourceCurie, err = s.GetNamespacedIdentifierFromUri(uri)
		if err != nil {
			return nil, err
		}
	}

	if predicate != "*" {
		if strings.HasPrefix(predicate, "ns") {
			predCurie = predicate
		} else {
			predCurie, err = s.GetNamespacedIdentifierFromUri(predicate)
			if err != nil {
				return nil, err
			}
		}
	}

	results := make([]result, 0)

	// lookup pred and id
	err = s.database.View(func(txn *badger.Txn) error {

		rid, ridExists, err := s.getIDForURI(txn, resourceCurie)
		if err != nil {
			return err
		}
		if !ridExists {
			return nil
		}

		var pid uint64
		var pidExists bool
		if predicate != "*" {
			pid, pidExists, err = s.getIDForURI(txn, predCurie)
			if err != nil {
				return err
			}

			if !pidExists {
				return nil
			}
		}

		if inverse {
			opts1 := badger.DefaultIteratorOptions
			opts1.PrefetchValues = false
			opts1.Reverse = true
			outgoingIterator := txn.NewIterator(opts1)
			defer outgoingIterator.Close()

			searchBuffer := make([]byte, 10)
			binary.BigEndian.PutUint16(searchBuffer, INCOMING_REF_INDEX)
			binary.BigEndian.PutUint64(searchBuffer[2:], rid)
			searchBuffer = append(searchBuffer, 0xFF)
			// incoming buffer ic-indexid:relatedid:rid:time:pred::deleted => nil

			prefixBuffer := make([]byte, 10)
			binary.BigEndian.PutUint16(prefixBuffer, INCOMING_REF_INDEX)
			binary.BigEndian.PutUint64(prefixBuffer[2:], rid)

			// iterate all incoming refs
			// continue if predicate is wrong
			// for first rid of same entity return if not deleted.
			// while rid is same continue.
			var currentRID uint64
			currentRID = 0
			candidates := 0
			seenForDataset := make(map[uint32]uint64)
			for outgoingIterator.Seek(searchBuffer); outgoingIterator.ValidForPrefix(prefixBuffer); outgoingIterator.Next() {
				candidates++
				item := outgoingIterator.Item()
				k := item.Key()

				// check dataset if deleted, or if excluded from search
				datasetId := binary.BigEndian.Uint32(k[36:])

				datasetIncluded := len(targetDatasetIds) == 0 //no specified datasets means no restriction - all datasets are allowed
				if !datasetIncluded {
					for _, id := range targetDatasetIds {
						if id == datasetId {
							datasetIncluded = true
							break
						}
					}
				}

				if s.deletedDatasets[datasetId] || !datasetIncluded {
					continue
				}

				// get recorded time on relationship
				// skip over all entries until et gt than query time
				et := int64(binary.BigEndian.Uint64(k[18:]))
				if et > queryTime {
					continue
				}

				// get related
				relatedID := binary.BigEndian.Uint64(k[10:])

				if relatedID != currentRID {
					seenForDataset = make(map[uint32]uint64)
				}

				// set current to be this related object
				currentRID = relatedID

				if _, found := seenForDataset[datasetId]; found {
					continue
				} else {
					seenForDataset[datasetId] = currentRID
				}

				// get predicate
				predID := binary.BigEndian.Uint64(k[26:])
				if pid != predID && predicate != "*" {
					continue
				}

				// check is deleted
				del := binary.BigEndian.Uint16(k[34:])
				if del == 1 {
					continue
				}

				// add to results
				results = append(results, result{time: uint64(et), entityID: relatedID, predicateID: predID, datasetID: datasetId})
			}
		} else {
			opts1 := badger.DefaultIteratorOptions
			opts1.PrefetchValues = false
			opts1.Reverse = true
			outgoingIterator := txn.NewIterator(opts1)
			defer outgoingIterator.Close()

			searchBuffer := make([]byte, 18)
			binary.BigEndian.PutUint16(searchBuffer, OUTGOING_REF_INDEX)
			binary.BigEndian.PutUint64(searchBuffer[2:], rid)
			binary.BigEndian.PutUint64(searchBuffer[10:], uint64(queryTime))
			searchBuffer = append(searchBuffer, 0xFF) // add the wildcard so we get equals matches as well

			prefixBuffer := make([]byte, 10)
			binary.BigEndian.PutUint16(prefixBuffer, OUTGOING_REF_INDEX)
			binary.BigEndian.PutUint64(prefixBuffer[2:], rid)

			candidates := 0
			datasetTimestamps := make(map[uint32]uint64)
			for outgoingIterator.Seek(searchBuffer); outgoingIterator.ValidForPrefix(prefixBuffer); outgoingIterator.Next() {
				candidates++
				item := outgoingIterator.Item()
				k := item.Key()

				datasetId := binary.BigEndian.Uint32(k[36:])

				datasetIncluded := len(targetDatasetIds) == 0 //no specified datasets means no restriction - all datasets are allowed
				if !datasetIncluded {
					for _, id := range targetDatasetIds {
						if id == datasetId {
							datasetIncluded = true
							break
						}
					}
				}

				if s.deletedDatasets[datasetId] || !datasetIncluded {
					continue
				}

				// get time
				et := binary.BigEndian.Uint64(k[10:])

				// if time has changed for given dataset id then carry on
				if v, found := datasetTimestamps[datasetId]; found {
					if v != et {
						continue
					}
				} else {
					datasetTimestamps[datasetId] = et
				}

				// get predicate
				predID := binary.BigEndian.Uint64(k[18:])
				if pid != predID && predicate != "*" {
					continue
				}

				del := binary.BigEndian.Uint16(k[34:])
				if del == 1 {
					continue
				}

				// get related
				relatedID := binary.BigEndian.Uint64(k[26:])

				// add to results
				results = append(results, result{time: et, entityID: relatedID, predicateID: predID})
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
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
	binary.BigEndian.PutUint16(uribuf, URI_TO_ID_INDEX_ID)
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

func (s *Store) commitIdTxn() error {
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
	binary.BigEndian.PutUint16(buf, ID_TO_URI_INDEX_ID)
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
	binary.BigEndian.PutUint16(uribuf, URI_TO_ID_INDEX_ID)
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
			binary.BigEndian.PutUint16(invseqbuf, ID_TO_URI_INDEX_ID)
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

		val, err = item.ValueCopy(nil)
		return nil
	})

	if err != nil {
		panic(err.Error())
	}

	tags := []string{
		"application:datahub",
	}

	if len(val) > 0 {
		_ = s.statsdClient.Count("store.read.bytes", int64(len(val)), tags, 1) // don't care about errors here
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
	if data == nil || len(data) == 0 {
		obj = nil
		return nil
	}

	err := json.Unmarshal(data, obj)
	if err != nil {
		s.logger.Errorw("GetObject error: %", err.Error())
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
