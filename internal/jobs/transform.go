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
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/gojektech/heimdall/v6/httpclient"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
)

type Transform interface {
	GetConfig() map[string]interface{}
	transformEntities(runner *Runner, entities []*server.Entity) ([]*server.Entity, error)
}

// these are upper cased to prevent the user from accidentally redefining them
// (i mean, not really, but maybe it will help)
const helperJavascriptFunctions = `
function SetProperty(entity, prefix, name, value) {
	entity["Properties"][prefix+":"+name] = value;
}
function GetProperty(entity, prefix, name) {
	return entity["Properties"][prefix+":"+name];
}
function AddReference(entity, prefix, name, value) {
	entity["References"][prefix+":"+name] = value;
}
function GetId(entity) {
    return entity["ID"];
}
function PrefixField(prefix, field) {
    return prefix + ":" + field;
}
function RenameProperty(entity, originalPrefix, originalName, newPrefix, newName) {
	var value = GetProperty(entity, originalPrefix, originalName);
	SetProperty(entity, newPrefix, newName, value);
	RemoveProperty(entity, originalPrefix, originalName);
}
function ToString(value){
	return (value === null || value === undefined) ? value : value.toString();
}
function RemoveProperty(entity, prefix, name){
	delete entity["Properties"][prefix+":"+name];
}
`

func (s *Scheduler) parseTransform(config *JobConfiguration) (Transform, error) {
	transformConfig := config.Transform
	if transformConfig != nil {
		transformTypeName := transformConfig["Type"]
		if transformTypeName != nil {
			if transformTypeName == "HttpTransform" {
				transform := &HttpTransform{}
				endpoint, ok := transformConfig["Url"]
				if ok && endpoint != "" {
					transform.Endpoint = endpoint.(string)
				}
				tokenProvider, ok := transformConfig["TokenProvider"]
				if ok {
					transform.TokenProvider = tokenProvider.(string)
				}
				return transform, nil
			} else if transformTypeName == "JavascriptTransform" {
				code64, ok := transformConfig["Code"]
				if ok && code64 != "" {
					transform, err := newJavascriptTransform(s.Logger, code64.(string), s.Store)
					if err != nil {
						return nil, err
					}
					return transform, nil
				}
				return nil, nil
			}
			return nil, errors.New("unknown transform type: " + transformTypeName.(string))
		}
		return nil, errors.New("transform config must contain 'Type'. can be one of: JavascriptTransform, HttpTransform")
	}
	return nil, nil
}

func newJavascriptTransform(log *zap.SugaredLogger, code64 string, store *server.Store) (*JavascriptTransform, error) {
	transform := &JavascriptTransform{Logger: log.Named("transform")}
	code, err := base64.StdEncoding.DecodeString(code64)
	if err != nil {
		return nil, err
	}
	transform.Code = code
	transform.Runtime = goja.New()
	transform.Store = store

	// add query function to runtime
	transform.Runtime.Set("Query", transform.Query)
	transform.Runtime.Set("FindById", transform.ById)
	transform.Runtime.Set("GetNamespacePrefix", transform.GetNamespacePrefix)
	transform.Runtime.Set("AssertNamespacePrefix", transform.AssertNamespacePrefix)
	transform.Runtime.Set("Log", transform.Log)
	transform.Runtime.Set("NewEntity", transform.NewEntity)

	_, err = transform.Runtime.RunString(string(code))
	if err != nil {
		return nil, err
	}

	// add helper functions
	_, err = transform.Runtime.RunString(helperJavascriptFunctions)
	if err != nil {
		return nil, err
	}
	return transform, nil
}

type JavascriptTransform struct {
	Store   *server.Store
	Code    []byte
	Runtime *goja.Runtime
	Logger  *zap.SugaredLogger
}

func (javascriptTransform *JavascriptTransform) MakeTransaction() *server.Transaction {
	txn := &server.Transaction{}
	txn.DatasetEntities = make(map[string][]*server.Entity)
	return txn
}

func (javascriptTransform *JavascriptTransform) RunTransaction(txn *server.Transaction) {
	javascriptTransform.Store.ExecuteTransaction(txn)
}

func (javascriptTransform *JavascriptTransform) Log(thing interface{}) {
	javascriptTransform.Logger.Info(thing)
}

func (javascriptTransform *JavascriptTransform) MakeEntityArray(entities []interface{}) []*server.Entity {
	newArray := make([]*server.Entity, 0)
	for _, e := range entities {
		newArray = append(newArray, e.(*server.Entity))
	}
	return newArray
}

func (javascriptTransform *JavascriptTransform) NewEntity() *server.Entity {
	entity := &server.Entity{}
	entity.References = map[string]interface{}{}
	entity.Properties = map[string]interface{}{}
	entity.InternalID = 0
	return entity
}

func (javascriptTransform *JavascriptTransform) GetNamespacePrefix(urlExpansion string) string {
	prefix, _ := javascriptTransform.Store.NamespaceManager.GetPrefixMappingForExpansion(urlExpansion)
	return prefix
}

func (javascriptTransform *JavascriptTransform) AssertNamespacePrefix(urlExpansion string) string {
	prefix, _ := javascriptTransform.Store.NamespaceManager.AssertPrefixMappingForExpansion(urlExpansion)
	return prefix
}

func (javascriptTransform *JavascriptTransform) Query(startingEntities []string, predicate string, inverse bool, datasets []string) [][]interface{} {
	results, err := javascriptTransform.Store.GetManyRelatedEntities(startingEntities, predicate, inverse, datasets)
	if err != nil {
		return nil
	}
	return results
}

func (javascriptTransform *JavascriptTransform) ById(entityId string, datasets []string) *server.Entity {
	entity, err := javascriptTransform.Store.GetEntity(entityId, datasets)
	if err != nil {
		return nil
	}
	return entity
}

func (javascriptTransform *JavascriptTransform) transformEntities(runner *Runner, entities []*server.Entity) ([]*server.Entity, error) {

	var transformFunc func(entities []*server.Entity) (interface{}, error)
	err := javascriptTransform.Runtime.ExportTo(javascriptTransform.Runtime.Get("transform_entities"), &transformFunc)

	if err != nil {
		return nil, err
	}

	// invoke transform, and catch js runtime err
	result, err := transformFunc(entities)
	if err != nil {
		return nil, err
	}

	var resultEntities []*server.Entity
	switch v := result.(type) {
	case []interface{}:
		resultEntities = make([]*server.Entity, 0)
		for _, e := range v {
			resultEntities = append(resultEntities, e.(*server.Entity))
		}
	case []*server.Entity:
		resultEntities = v
	default:
		return nil, errors.New("bad result from transform")
	}

	return resultEntities, nil
}

func (javascriptTransform *JavascriptTransform) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "JavascriptTransform"
	config["Code"] = base64.StdEncoding.EncodeToString(javascriptTransform.Code)
	return config
}

type HttpTransform struct {
	Endpoint       string
	Authentication string // "none, basic, token"
	User           string // for use in basic auth
	Password       string // for use in basic auth
	TokenProvider  string // for use in token auth
}

func (httpTransform *HttpTransform) transformEntities(runner *Runner, entities []*server.Entity) ([]*server.Entity, error) {
	timeout := 1000 * time.Millisecond
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// create headers if needed
	url := httpTransform.Endpoint

	// set up our request
	jsonEntities, err := json.Marshal(entities)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(jsonEntities)
	req, err := http.NewRequest("POST", url, r) //
	if err != nil {
		return nil, err
	}
	req.Close = true

	// security
	if httpTransform.TokenProvider != "" {
		// attempt to parse the token provider
		provider, ok := runner.tokenProviders.Providers[strings.ToLower(httpTransform.TokenProvider)]
		if ok {
			tokenProvider := provider.(security.TokenProvider)
			bearer, err := tokenProvider.Token()
			if err != nil {
				runner.logger.Warnf("Token provider returned error: %w", err)
			}
			req.Header.Add("Authorization", bearer)
		}
	}

	// do post to transform
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, handleHttpError(res)
	}

	// parse json back into []*Entity
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var transformedEntities []*server.Entity
	err = json.Unmarshal(body, &transformedEntities)
	if err != nil {
		return nil, err
	}

	return transformedEntities, nil
}

func (httpTransform *HttpTransform) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "HttpTransform"
	config["Url"] = httpTransform.Endpoint
	config["TokenProvider"] = httpTransform.TokenProvider
	config["Authentication"] = httpTransform.Authentication
	return config
}
