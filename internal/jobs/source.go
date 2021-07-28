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

package jobs

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
)

// Source interface for pulling data
type Source interface {
	GetConfig() map[string]interface{}
	readEntities(runner *Runner, since string, batchSize int, processEntities func([]*server.Entity, string) error) error
	startFullSync()
	endFullSync()
}

func (s *Scheduler) parseSource(jobConfig *JobConfiguration) (Source, error) {
	sourceConfig := jobConfig.Source
	if sourceConfig != nil {
		sourceTypeName := sourceConfig["Type"]
		if sourceTypeName != nil {
			if sourceTypeName == "HttpDatasetSource" {
				source := &httpDatasetSource{}
				source.Store = s.Store
				endpoint, ok := sourceConfig["Url"]
				if ok && endpoint != "" {
					source.Endpoint = endpoint.(string)
				}
				tokenProvider, ok := sourceConfig["TokenProvider"]
				if ok {
					source.TokenProvider = tokenProvider.(string)
				}
				return source, nil
			} else if sourceTypeName == "DatasetSource" {
				source := &datasetSource{}
				source.Store = s.Store
				source.DatasetManager = s.DatasetManager
				source.DatasetName = (sourceConfig["Name"]).(string)
				return source, nil
			} else if sourceTypeName == "MultiSource" {
				source := &multiSource{}

				return source, nil
			} else if sourceTypeName == "SampleSource" {
				source := &sampleSource{}
				source.Store = s.Store
				numEntities := sourceConfig["NumberOfEntities"]
				if numEntities != nil {
					source.NumberOfEntities = int(numEntities.(float64))
				}
				return source, nil
			} else if sourceTypeName == "SlowSource" {
				source := &slowSource{}
				source.Sleep = sourceConfig["Sleep"].(string)
				batch := sourceConfig["BatchSize"]
				if batch != nil {
					source.BatchSize = int(batch.(float64))
				}
				return source, nil
			} else {
				return nil, errors.New("unknown source type: " + sourceTypeName.(string))
			}
		}
		return nil, errors.New("missing source type")
	}
	return nil, errors.New("missing source config")

}

type join struct {
	dataset  string
	property string
	inverse  bool
}

type dependency struct {
	dataset string // name of the dependent dataset
	joins   []join
}

type multiSource struct {
	mainDataset       string       // the main dataset that is read on initial sync
	dependencies      []dependency // the dependency queries
	enableTracking    bool
	mainDatasetSynced bool
}

type multiSourceContinuationToken struct {
	dependenciesSince []string //
}

func (multiSource *multiSource) startFullSync() {
}

func (multiSource *multiSource) endFullSync() {
}

func (multiSource *multiSource) readEntities(runner *Runner, since string, batchSize int, processEntities func([]*server.Entity, string) error) error {
	//

	return nil
}

type datasetSource struct {
	DatasetName    string
	Store          *server.Store
	DatasetManager *server.DsManager
	isFullSync     bool
}

func (datasetSource *datasetSource) startFullSync() {
	datasetSource.isFullSync = true
}

func (datasetSource *datasetSource) endFullSync() {
	datasetSource.isFullSync = false
}

func (datasetSource *datasetSource) readEntities(runner *Runner, since string, batchSize int, processEntities func([]*server.Entity, string) error) error {
	exists := datasetSource.DatasetManager.IsDataset(datasetSource.DatasetName)
	if !exists {
		return errors.New("dataset is missing")
	}
	dataset := datasetSource.DatasetManager.GetDataset(datasetSource.DatasetName)

	sinceInt := 0

	entities := make([]*server.Entity, 0)
	if datasetSource.isFullSync {
		continuation, err := dataset.MapEntities(since, batchSize, func(entity *server.Entity) error {
			entities = append(entities, entity)
			return nil
		})
		if err != nil {
			return err
		}

		err = processEntities(entities, continuation)
		if err != nil {
			return err
		}
	} else {
		if since != "" {
			s, err := strconv.Atoi(since)
			if err != nil {
				return err
			}
			sinceInt = s
		}
		continuation, err := dataset.ProcessChanges(uint64(sinceInt), batchSize, func(entity *server.Entity) {
			entities = append(entities, entity)
		})
		if err != nil {
			return err
		}

		err = processEntities(entities, strconv.Itoa(int(continuation)))
		if err != nil {
			return err
		}
	}

	return nil
}

func (datasetSource *datasetSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "DatasetSource"
	config["Name"] = datasetSource.DatasetName
	return config
}

type httpDatasetSource struct {
	Endpoint       string
	Authentication string // "none, basic, token"
	User           string // for use in basic auth
	Password       string // for use in basic auth
	TokenProvider  string // for use in token auth
	Store          *server.Store
}

func (httpDatasetSource *httpDatasetSource) startFullSync() {
	// empty for now (this makes sonar not complain)
}

func (httpDatasetSource *httpDatasetSource) endFullSync() {
	// empty for now (this makes sonar not complain)
}

func (httpDatasetSource *httpDatasetSource) readEntities(runner *Runner, since string, batchSize int, processEntities func([]*server.Entity, string) error) error {
	// open connection
	//timeout := 60 * time.Minute
	//client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// create headers if needed
	endpoint, err := url.Parse(httpDatasetSource.Endpoint)
	if err != nil {
		return err
	}
	if since != "" {
		q, _ := url.ParseQuery(endpoint.RawQuery)
		q.Add("since", since)
		endpoint.RawQuery = q.Encode()
	}

	// set up our request
	req, err := http.NewRequest("GET", endpoint.String(), nil) //
	if err != nil {
		return err
	}

	// we add a cancellable context, and makes sure it gets cancelled when we exit
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a transport with sane defaults, but with a default content timeout of 0 (infinite)
	var netTransport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	var netClient = &http.Client{
		Transport: netTransport,
	}

	// security
	if httpDatasetSource.TokenProvider != "" {
		// attempt to parse the token provider
		provider, ok := runner.tokenProviders.Providers[strings.ToLower(httpDatasetSource.TokenProvider)]
		if ok {
			tokenProvider := provider.(security.TokenProvider)
			bearer, err := tokenProvider.Token()
			if err != nil {
				runner.logger.Warnf("Token provider returned error: %w", err)
			}
			req.Header.Add("Authorization", bearer)
		}
	}

	// do get
	res, err := netClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return handleHttpError(res)
	}

	// set default batch size if not specified
	if batchSize == 0 {
		batchSize = 100
	}

	read := 0
	entities := make([]*server.Entity, 0)
	continuationToken := ""
	esp := server.NewEntityStreamParser(httpDatasetSource.Store)
	err = esp.ParseStream(res.Body, func(entity *server.Entity) error {
		if entity.ID == "@continuation" {
			continuationToken = entity.GetStringProperty("token")
		} else {
			entities = append(entities, entity)
			read++
			if read == batchSize {
				read = 0
				err := processEntities(entities, continuationToken)
				if err != nil {
					return err
				}
				entities = make([]*server.Entity, 0)
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	if read > 0 {
		err = processEntities(entities, continuationToken)
		if err != nil {
			return err
		}
	} else {
		_ = processEntities(make([]*server.Entity, 0), continuationToken)
	}

	return nil
}

func (httpDatasetSource *httpDatasetSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "HttpDatasetSource"
	config["Url"] = httpDatasetSource.Endpoint
	config["TokenProvider"] = httpDatasetSource.TokenProvider
	return config
}

type sampleSource struct {
	NumberOfEntities int
	BatchSize        int
	Store            *server.Store
}

func (source *sampleSource) startFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *sampleSource) endFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *sampleSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "SampleSource"
	config["NumberOfEntities"] = source.NumberOfEntities
	return config
}

func (source *sampleSource) readEntities(runner *Runner, since string, batchSize int, processEntities func([]*server.Entity, string) error) error {

	var sinceOffset = 0
	var err error
	if since != "" {
		sinceOffset, err = strconv.Atoi(since)
		if err != nil {
			return err
		}
	}
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

	sinceToken := strconv.Itoa(endIndex)
	return processEntities(entities, sinceToken)
}

type slowSource struct {
	BatchSize int
	Sleep     string
}

func (source *slowSource) startFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *slowSource) endFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *slowSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "SlowSource"
	config["BatchSize"] = source.BatchSize
	config["Sleep"] = source.Sleep

	return config
}

func (source *slowSource) readEntities(runner *Runner, since string, batchSize int, processEntities func([]*server.Entity, string) error) error {
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

	return processEntities(entities, "")
}
