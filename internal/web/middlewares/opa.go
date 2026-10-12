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

package middlewares

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gojektech/heimdall/v6/httpclient"
	"github.com/golang-jwt/jwt/v4"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type opaAnswer struct {
	Result bool `json:"result"`
}

type opaRequest struct {
	Input map[string]interface{} `json:"input"`
}

type opaDatasets struct {
	Result json.RawMessage `json:"result"`
}

func doOpaCheck(logger *zap.SugaredLogger, method string, path string, token *jwt.Token, scopes []string, opaEndpoint string) ([]string, error) {
	input := opaRequest{
		Input: map[string]interface{}{
			"method": method,
			"path":   path,
			"token":  token.Raw,
			"scopes": scopes,
		},
	}

	body, err := opaQuery(fmt.Sprintf("%s/v1/data/datahub/authz/allow", opaEndpoint), input)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusForbidden, err.Error())
	}

	answer := opaAnswer{}
	err = json.Unmarshal(body, &answer)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusForbidden, err.Error())
	}
	if !answer.Result {
		return nil, echo.NewHTTPError(http.StatusForbidden, "user has no access to resource")
	}

	// Determine the permitted datasets
	body, err = opaQuery(fmt.Sprintf("%s/v1/data/datahub/authz/datasets", opaEndpoint), input)
	if err != nil {
		logger.Errorf("opa query failed, result|err: %s %+v", string(body), err)

		return nil, echo.NewHTTPError(http.StatusForbidden, err.Error())
	}

	return parseDatasetsFromOpaBody(logger, body)
}

// parseDatasetsFromOpaBody parses the response body from OPA to extract datasets.
// The result is either a map of dataset name to bool, where only true grants access,
// or a list of dataset names. A "*" key in the map decides alone: true means access
// to all datasets, and false returns an error so the caller falls back to the ACL check.
func parseDatasetsFromOpaBody(logger *zap.SugaredLogger, opaBody []byte) ([]string, error) {
	resp := opaDatasets{}
	if err := json.Unmarshal(opaBody, &resp); err != nil {
		logger.Warnf("opaDatasets error, result|err: %s %+v", string(opaBody), err)
		return nil, err
	}

	datasets := make([]string, 0)
	if len(resp.Result) == 0 {
		return datasets, nil
	}

	allowed := map[string]bool{}
	if err := json.Unmarshal(resp.Result, &allowed); err == nil {
		if isAdmin, ok := allowed["*"]; ok {
			if !isAdmin {
				return nil, fmt.Errorf("OPA denied admin access")
			}
			return []string{"*"}, nil
		}
		for name, ok := range allowed {
			if ok {
				datasets = append(datasets, name)
			}
		}
		return datasets, nil
	}

	if err := json.Unmarshal(resp.Result, &datasets); err != nil {
		logger.Warnf("opaDatasets error, result|err: %s %+v", string(opaBody), err)
		return nil, fmt.Errorf("failed to parse OPA response as either dataset map or dataset list: %w", err)
	}

	return datasets, nil
}

func opaQuery(url string, request opaRequest) ([]byte, error) {
	timeout := 1000 * time.Millisecond
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// set up our request
	jsonEntities, err := json.Marshal(&request)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(jsonEntities)

	req, err := http.NewRequest("POST", url, r) //
	if err != nil {
		return nil, err
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = res.Body.Close()
	}()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return bodyBytes, nil
}
