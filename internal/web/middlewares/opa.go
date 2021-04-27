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
	"github.com/goccy/go-json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gojektech/heimdall/v6/httpclient"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type opaAnswer struct {
	Result bool `json:"result"`
}

type opaRequest struct {
	Input map[string]interface{} `json:"input"`
}

type opaDataset struct {
	Name string `json:"name"`
}

type opaDatasets struct {
	Result []string `json:"result"`
}

func OpaAuthorizer(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {
	opaEndpoint := viper.GetString("OPA_ENDPOINT")

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {

			token := c.Get("user").(*jwt.Token)

			input := opaRequest{
				Input: map[string]interface{}{
					"method": c.Request().Method,
					"path":   c.Request().URL.Path,
					"token":  token.Raw,
					"scopes": scopes,
				},
			}

			body, err := opaQuery(fmt.Sprintf("%s/v1/data/datahub/authz/allow", opaEndpoint), input)
			if err != nil {
				return echo.NewHTTPError(http.StatusForbidden, err.Error())
			}

			answer := opaAnswer{}
			err = json.Unmarshal(body, &answer)
			if err != nil {
				return echo.NewHTTPError(http.StatusForbidden, err.Error())
			}
			if !answer.Result {
				return echo.NewHTTPError(http.StatusForbidden, "user has no access to resource")
			}

			// lets figure out the users datasets
			body, err = opaQuery(fmt.Sprintf("%s/v1/data/datahub/authz/datasets", opaEndpoint), input)
			if err != nil {
				return echo.NewHTTPError(http.StatusForbidden, err.Error())
			}
			resp := opaDatasets{}
			err = json.Unmarshal(body, &resp)
			datasets := pluckDatasets(resp)
			c.Set("datasets", datasets)

			return next(c)
		}
	}
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

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return bodyBytes, nil
}

// pluckDatasets is used to make sure we don't accidentally end up with a result
// that breaks the endpoint
func pluckDatasets(resp opaDatasets) []string {
	datasets := make([]string, 0)

	if len(resp.Result) > 0 {
		for _, item := range resp.Result {
			datasets = append(datasets, item)
		}
	}
	return datasets

}
