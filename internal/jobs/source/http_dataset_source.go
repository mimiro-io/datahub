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

package source

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
)

type HTTPDatasetSource struct {
	Endpoint       string
	Authentication string            // "none, basic, token"
	User           string            // for use in basic auth
	Password       string            // for use in basic auth
	TokenProvider  security.Provider // for use in token auth
	Store          *server.Store
	Logger         *zap.SugaredLogger
}

func (httpDatasetSource *HTTPDatasetSource) StartFullSync() {
	// empty for now (this makes sonar not complain)
}

func (httpDatasetSource *HTTPDatasetSource) EndFullSync() {
	// empty for now (this makes sonar not complain)
}

func (httpDatasetSource *HTTPDatasetSource) ReadEntities(ctx context.Context, since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	// open connection
	// timeout := 60 * time.Minute
	// client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// create headers if needed
	endpoint, err := url.Parse(httpDatasetSource.Endpoint)
	if err != nil {
		return err
	}
	if since.GetToken() != "" {
		q, _ := url.ParseQuery(endpoint.RawQuery)
		q.Add("since", since.GetToken())
		endpoint.RawQuery = q.Encode()
	}

	// set up our request
	req, err := http.NewRequest("GET", endpoint.String(), nil) //
	if err != nil {
		return err
	}

	// we add a cancellable context, and makes sure it gets cancelled when we exit
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	netClient := httpClient()

	// security
	if httpDatasetSource.TokenProvider != nil {
		httpDatasetSource.TokenProvider.Authorize(req)
	}

	// do get
	res, err := netClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return handleHTTPError(res)
	}

	// set default batch size if not specified
	if batchSize == 0 {
		batchSize = 100
	}

	read := 0
	entities := make([]*server.Entity, 0)
	continuationToken := &StringDatasetContinuation{}
	esp := server.NewEntityStreamParser(httpDatasetSource.Store)
	err = esp.ParseStream(res.Body, func(entity *server.Entity) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entity.ID == "@continuation" {
			continuationToken.Token = entity.GetStringProperty("token")
		} else {
			entities = append(entities, entity)
			read++
			if read == batchSize {
				read = 0
				err2 := processEntities(entities, continuationToken)
				if err2 != nil {
					return err2
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

var globalHttpClient *http.Client

// use common http client for all http sources, to reuse connections
func httpClient() *http.Client {
	if globalHttpClient == nil {
		// set up a transport with sane defaults, but with a default content timeout of 0 (infinite)
		netTransport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 5 * time.Second,
				// keep connections alive a short time
				KeepAliveConfig: net.KeepAliveConfig{
					Enable:   true,
					Idle:     15 * time.Second,
					Interval: 15 * time.Second,
					Count:    3,
				},
			}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
			MaxIdleConnsPerHost: 1000,
			MaxConnsPerHost:     1000,
		}
		netClient := &http.Client{
			Transport: netTransport,
		}
		globalHttpClient = netClient
	}
	return globalHttpClient
}

func (httpDatasetSource *HTTPDatasetSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "HttpDatasetSource"
	config["Url"] = httpDatasetSource.Endpoint
	config["TokenProvider"] = httpDatasetSource.TokenProvider
	return config
}

func handleHTTPError(response *http.Response) error {
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("received http source error (%d). Unable to read error body", response.StatusCode)
	}
	return fmt.Errorf("received http source error (%d): %s", response.StatusCode, string(bodyBytes))
}
