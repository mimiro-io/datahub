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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/gojektech/heimdall/v6/httpclient"
	"github.com/google/uuid"
	"github.com/mimiro-io/datahub/internal/server"
)

// Interface defs
// Sink interface for where data can be pushed
type Sink interface {
	GetConfig() map[string]interface{}
	processEntities(runner *Runner, entities []*server.Entity) error
	startFullSync(runner *Runner) error
	endFullSync(runner *Runner) error
}

func (s *Scheduler) parseSink(jobConfig *JobConfiguration) (Sink, error) {
	sinkConfig := jobConfig.Sink
	if sinkConfig != nil {
		sinkTypeName := sinkConfig["Type"]
		if sinkTypeName != nil {
			if sinkTypeName == "DatasetSink" {
				dsname := (sinkConfig["Name"]).(string)
				dataset := s.DatasetManager.GetDataset(dsname)
				if dataset != nil && dataset.IsProxy() {
					sink := &httpDatasetSink{}
					sink.Store = s.Store
					sink.logger = s.Logger.Named("sink")
					sink.Endpoint, _ = server.UrlJoin(dataset.ProxyConfig.RemoteUrl, "/entities")

					if dataset.ProxyConfig.AuthProviderName != "" {
						sink.TokenProvider = dataset.ProxyConfig.AuthProviderName
					}
					return sink, nil
				}
				sink := &datasetSink{}
				sink.DatasetName = dsname
				sink.Store = s.Store
				sink.DatasetManager = s.DatasetManager
				return sink, nil
			} else if sinkTypeName == "DevNullSink" {
				sink := &devNullSink{}
				return sink, nil
			} else if sinkTypeName == "ConsoleSink" {
				sink := &consoleSink{}
				v, ok := sinkConfig["Prefix"]
				if ok {
					sink.Prefix = v.(string)
				}
				v, ok = sinkConfig["Detailed"]
				if ok {
					sink.Detailed = v.(bool)
				}

				return sink, nil
			} else if sinkTypeName == "HttpDatasetSink" {
				sink := &httpDatasetSink{}
				sink.Store = s.Store
				sink.logger = s.Logger.Named("sink")

				endpoint, ok := sinkConfig["Url"]
				if ok && endpoint != "" {
					sink.Endpoint = endpoint.(string)
				}
				tokenProvider, ok := sinkConfig["TokenProvider"]
				if ok {
					sink.TokenProvider = tokenProvider.(string)
				}
				return sink, nil
			} else {
				return nil, errors.New("unknown sink type: " + sinkTypeName.(string))
			}
		}
		return nil, errors.New("missing sink type")
	}
	return nil, errors.New("missing or wrong sink type")

}

type devNullSink struct{}

func (devNullSink *devNullSink) startFullSync(runner *Runner) error {
	return nil
}

func (devNullSink *devNullSink) endFullSync(runner *Runner) error {
	return nil
}

func (devNullSink *devNullSink) processEntities(*Runner, []*server.Entity) error {
	return nil
}

func (devNullSink *devNullSink) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "DevNullSink"
	return config
}

type consoleSink struct {
	Prefix   string // prefix for the log entries
	Detailed bool   // if true, logs all entries
}

func (consoleSink *consoleSink) startFullSync(runner *Runner) error {
	return nil
}

func (consoleSink *consoleSink) endFullSync(runner *Runner) error {
	return nil
}

func (consoleSink *consoleSink) processEntities(runner *Runner, entities []*server.Entity) error {
	now := time.Now()
	if consoleSink.Detailed {
		for _, e := range entities {
			runner.logger.Infof("%s - %sProcessing %s entities", now.Format(time.RFC3339), consoleSink.Prefix, e)
		}
	} else {
		runner.logger.Infof("%s - %sProcessing %d entities", now.Format(time.RFC3339), consoleSink.Prefix, len(entities))
	}
	return nil
}

func (consoleSink *consoleSink) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "ConsoleSink"
	config["Prefix"] = consoleSink.Prefix
	config["Detailed"] = consoleSink.Detailed
	return config
}

type httpDatasetSink struct {
	Endpoint         string // endpoint for writable dataset
	Authentication   string // "none, basic, token"
	User             string // for use in basic auth
	Password         string // for use in basic auth
	TokenProvider    string // for use in token auth
	ExpandNamespaces bool   // used to instruct the sink to expand all namespaces
	StripPrefixes    bool   // used to instruct the sink to remove prefixes
	Store            *server.Store
	inFullSync       bool
	fullSyncID       string
	isFirstBatch     bool
	logger           *zap.SugaredLogger
}

func (httpDatasetSink *httpDatasetSink) startFullSync(runner *Runner) error {
	httpDatasetSink.isFirstBatch = true
	id := uuid.New()
	httpDatasetSink.fullSyncID = "fsid-" + id.String()
	httpDatasetSink.inFullSync = true
	return nil
}

func (httpDatasetSink *httpDatasetSink) endFullSync(runner *Runner) error {

	// send empty batch to server with end
	timeout := 60 * time.Minute
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// create headers if needed
	url := httpDatasetSink.Endpoint

	allObjects := make([]interface{}, 1)
	//TODO: https://mimiro.atlassian.net/browse/MIM-693
	allObjects[0] = runner.store.GetGlobalContext()

	jsonEntities, err := json.Marshal(allObjects)
	if err != nil {
		return err
	}
	r := bytes.NewReader(jsonEntities)
	req, err := http.NewRequest("POST", url, r) //
	if err != nil {
		return err
	}

	// add full sync headers
	req.Header.Add("universal-data-api-full-sync-end", "true")
	req.Header.Add("universal-data-api-full-sync-id", httpDatasetSink.fullSyncID)

	if httpDatasetSink.TokenProvider != "" {
		// attempt to parse the token provider
		if provider, ok := runner.tokenProviders.Get(strings.ToLower(httpDatasetSink.TokenProvider)); ok {
			provider.Authorize(req)
		}
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return handleHttpError(res)
	}

	// reset full sync state
	httpDatasetSink.inFullSync = false
	httpDatasetSink.fullSyncID = ""
	httpDatasetSink.isFirstBatch = false

	return nil
}

func (httpDatasetSink *httpDatasetSink) processEntities(runner *Runner, entities []*server.Entity) error {

	timeout := 60 * time.Minute
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// create headers if needed
	url := httpDatasetSink.Endpoint

	allObjects := make([]interface{}, len(entities)+1)
	//TODO: https://mimiro.atlassian.net/browse/MIM-693
	allObjects[0] = runner.store.GetGlobalContext()
	for i := 0; i < len(entities); i++ {
		allObjects[i+1] = entities[i]
	}

	// set up our request
	jsonEntities, err := json.Marshal(allObjects)
	if err != nil {
		return err
	}
	r := bytes.NewReader(jsonEntities)
	req, err := http.NewRequest("POST", url, r) //
	if err != nil {
		return err
	}

	// add full sync headers if required
	if httpDatasetSink.inFullSync {
		// add sync id header
		req.Header.Add("universal-data-api-full-sync-id", httpDatasetSink.fullSyncID)

		if httpDatasetSink.isFirstBatch {
			// add start header and set first to false
			req.Header.Add("universal-data-api-full-sync-start", "true")
			httpDatasetSink.isFirstBatch = false
		}
	}

	// security
	if httpDatasetSink.TokenProvider != "" {
		// attempt to parse the token provider
		if provider, ok := runner.tokenProviders.Get(strings.ToLower(httpDatasetSink.TokenProvider)); ok {
			provider.Authorize(req)
		}
	}

	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return handleHttpError(res)
	}

	return nil
}

func (httpDatasetSink *httpDatasetSink) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "HttpDatasetSink"
	config["Url"] = httpDatasetSink.Endpoint
	config["TokenProvider"] = httpDatasetSink.TokenProvider
	return config
}

type datasetSink struct {
	DatasetName    string
	Store          *server.Store
	DatasetManager *server.DsManager
}

func (datasetSink *datasetSink) startFullSync(runner *Runner) error {
	dataset := datasetSink.DatasetManager.GetDataset(datasetSink.DatasetName)
	if dataset == nil {
		return fmt.Errorf("dataset does not exist: %v", datasetSink.DatasetName)
	}
	return dataset.StartFullSync()
}

func (datasetSink *datasetSink) endFullSync(runner *Runner) error {
	dataset := datasetSink.DatasetManager.GetDataset(datasetSink.DatasetName)
	if dataset == nil {
		return fmt.Errorf("dataset does not exist: %v", datasetSink.DatasetName)
	}
	return dataset.CompleteFullSync()
}

func (datasetSink *datasetSink) processEntities(runner *Runner, entities []*server.Entity) error {
	exists := datasetSink.DatasetManager.IsDataset(datasetSink.DatasetName)
	if !exists {
		return fmt.Errorf("dataset does not exist: %v", datasetSink.DatasetName)
	}

	dataset := datasetSink.DatasetManager.GetDataset(datasetSink.DatasetName)

	err := dataset.StoreEntities(entities)

	if err == nil {
		// we emit the event so event handlers can react to it
		ctx := context.Background()
		runner.eventBus.Emit(ctx, "dataset."+datasetSink.DatasetName, nil)
		runner.eventBus.Emit(ctx, "dataset.core.Dataset", nil) // something has changed, should trigger this always
	}

	return err
}

func (datasetSink *datasetSink) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "DatasetSink"
	config["Name"] = datasetSink.DatasetName
	return config
}
func handleHttpError(response *http.Response) error {
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.New(fmt.Sprintf("Received http sink error (%d). Unable to read error body", response.StatusCode))
	}
	return errors.New(fmt.Sprintf("Received http sink error (%d): %s", response.StatusCode, string(bodyBytes)))
}
