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
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/gofrs/uuid"

	"github.com/dop251/goja"
	"github.com/gojektech/heimdall/v6/httpclient"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/server"
)

type Transform interface {
	GetConfig() map[string]interface{}
	transformEntities(runner *Runner, entities []*server.Entity, jobTag string) ([]*server.Entity, error)
	getParallelism() int
}

// these are upper cased to prevent the user from accidentally redefining them
// (i mean, not really, but maybe it will help)
const HelperJavascriptFunctions = `
function SetProperty(entity, prefix, name, value) {
	if (entity === null || entity === undefined) {
		return;
	}
	if (entity.Properties === null || entity.Properties === undefined) {
		return;
	}
	entity["Properties"][prefix+":"+name] = value;
}
function GetProperty(entity, prefix, name, defaultValue) {
	if (entity === null || entity === undefined) {
		return defaultValue;
	}
	if (entity.Properties === null || entity.Properties === undefined) {
		return defaultValue;
	}
	var value = entity["Properties"][prefix+":"+name]
	if (value === undefined || value === null) {
		return defaultValue;
	}
	return value;
}
function GetReference(entity, prefix, name, defaultValue) {
	if (entity === null || entity === undefined) {
		return defaultValue;
	}
	if (entity.References === null || entity.References === undefined) {
		return defaultValue;
	}
	var value = entity["References"][prefix+":"+name]
	if (value === undefined || value === null) {
		return defaultValue;
	}
	return value;
}
function AddReference(entity, prefix, name, value) {
	if (entity === null || entity === undefined) {
		return;
	}
	if (entity.References === null || entity.References === undefined) {
		return;
	}
	entity["References"][prefix+":"+name] = value;
}
function GetId(entity) {
	if (entity === null || entity === undefined) {
		return;
	}
	return entity["ID"];
}
function SetId(entity, id) {
	if (entity === null || entity === undefined) {
		return;
	}
	entity.ID = id
}

function SetDeleted(entity, deleted) {
	if (entity === null || entity === undefined) {
		return;
	}
	entity.IsDeleted = deleted
}

function GetDeleted(entity) {
	if (entity === null || entity === undefined) {
		return;
	}
	return entity.IsDeleted;
}

function PrefixField(prefix, field) {
    return prefix + ":" + field;
}
function RenameProperty(entity, originalPrefix, originalName, newPrefix, newName) {
	if (entity === null || entity === undefined) {
		return;
	}
	var value = GetProperty(entity, originalPrefix, originalName);
	SetProperty(entity, newPrefix, newName, value);
	RemoveProperty(entity, originalPrefix, originalName);
}

function RemoveProperty(entity, prefix, name){
	if (entity === null || entity === undefined) {
		return;
	}
	delete entity["Properties"][prefix+":"+name];
}

function NewEntityFrom(entity, addType, copyProps, copyRefs){
	if (entity === null || entity === undefined) {
		return NewEntity();
	}

	let newEntity = NewEntity();
	SetId(newEntity, GetId(entity));
	SetDeleted(newEntity, GetDeleted(entity));
	if (addType){
		let rdf = GetNamespacePrefix("http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		let type = GetReference(entity, rdf, "type");
		if (type != null){
			AddReference(newEntity, rdf, "type", type)
		}
	}
	if (copyProps) {
		for (const [key, value] of Object.entries(entity["Properties"])) {
			newEntity["Properties"][key] = value;
		}
	}
	if (copyRefs) {
		for (const [key, value] of Object.entries(entity["References"])) {
			newEntity["References"][key] = value;
		}
	}
	return newEntity;
}
`

func (s *Scheduler) parseTransform(config *JobConfiguration) (Transform, error) {
	transformConfig := config.Transform
	if transformConfig != nil {
		transformTypeName := transformConfig["Type"]
		if transformTypeName != nil {
			if transformTypeName == "HttpTransform" {
				transform := &HttpTransform{}
				url, ok := transformConfig["Url"]
				if ok && url != "" {
					transform.Url = url.(string)
				}
				tokenProvider, ok := transformConfig["TokenProvider"]
				if ok {
					transform.TokenProvider = tokenProvider.(string)
				}
				timeout, ok := transformConfig["TimeOut"]
				if ok && timeout != 0 {
					transform.TimeOut = timeout.(float64)
				} else {
					transform.TimeOut = 0
				}
				return transform, nil
			} else if transformTypeName == "JavascriptTransform" {
				code64, ok := transformConfig["Code"]
				if ok && code64 != "" {
					transform, err := NewJavascriptTransform(s.Logger, code64.(string), s.Store, s.DatasetManager)
					if err != nil {
						return nil, err
					}
					parallelism, ok := transformConfig["Parallelism"]
					if ok {
						transform.Parallelism = int(parallelism.(float64))
						if err != nil {
							return nil, err
						}
					} else {
						transform.Parallelism = 1
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

func NewJavascriptTransform(log *zap.SugaredLogger, code64 string, store *server.Store, dsm *server.DsManager) (*JavascriptTransform, error) {
	transform := &JavascriptTransform{Logger: log.Named("transform")}
	code, err := base64.StdEncoding.DecodeString(code64)
	if err != nil {
		return nil, err
	}
	transform.Code = code
	transform.Runtime = goja.New()
	transform.Store = store
	transform.DatasetManager = dsm

	// add query function to runtime
	transform.Runtime.Set("Query", transform.Query)
	transform.Runtime.Set("FindById", transform.ById)
	transform.Runtime.Set("GetNamespacePrefix", transform.GetNamespacePrefix)
	transform.Runtime.Set("AssertNamespacePrefix", transform.AssertNamespacePrefix)
	transform.Runtime.Set("Log", transform.Log)
	transform.Runtime.Set("NewEntity", transform.NewEntity)
	transform.Runtime.Set("ToString", transform.ToString)
	transform.Runtime.Set("Timing", transform.Timing)
	transform.Runtime.Set("NewTransaction", transform.NewTransaction)
	transform.Runtime.Set("ExecuteTransaction", transform.ExecuteTransaction)
	transform.Runtime.Set("AsEntity", transform.AsEntity)
	transform.Runtime.Set("UUID", transform.UUID)
	transform.Runtime.Set("WriteQueryResult", transform.WriteQueryResult)
	transform.Runtime.Set("GetDatasetChanges", transform.DatasetChanges)

	_, err = transform.Runtime.RunString(string(code))
	if err != nil {
		return nil, err
	}

	// add helper functions
	_, err = transform.Runtime.RunString(HelperJavascriptFunctions)
	if err != nil {
		return nil, err
	}
	return transform, nil
}

type JavascriptTransform struct {
	Store             *server.Store
	Code              []byte
	Runtime           *goja.Runtime
	Logger            *zap.SugaredLogger
	statsDClient      statsd.ClientInterface
	statsDTags        []string
	timings           map[string]time.Time
	Parallelism       int
	QueryResultWriter QueryResultWriter
	DatasetManager    *server.DsManager
}

func (javascriptTransform *JavascriptTransform) DatasetChanges(datasetName string, since uint64, limit int) (*server.Changes, error) {
	dataset := javascriptTransform.DatasetManager.GetDataset(datasetName)
	if dataset == nil {
		return nil, errors.New("dataset not found: " + datasetName)
	}

	changes, err := dataset.GetChanges(since, limit, true)
	return changes, err
}

func (javascriptTransform *JavascriptTransform) WriteQueryResult(object any) error {
	return javascriptTransform.QueryResultWriter.WriteObject(object)
}

func (javascriptTransform *JavascriptTransform) getParallelism() int {
	return javascriptTransform.Parallelism
}

// Clone the transform for use in parallel processing
func (javascriptTransform *JavascriptTransform) Clone() (*JavascriptTransform, error) {
	code := base64.StdEncoding.EncodeToString(javascriptTransform.Code)
	return NewJavascriptTransform(javascriptTransform.Logger, code, javascriptTransform.Store, javascriptTransform.DatasetManager)
}

func (javascriptTransform *JavascriptTransform) AsEntity(val interface{}) (res *server.Entity) {
	if e, ok := val.(*server.Entity); ok {
		res = e
		return
	}
	if m, ok := val.(map[string]interface{}); ok {
		defer func() {
			if recover() != nil {
				res = nil
			}
		}()
		res = server.NewEntityFromMap(m)
		return
	}
	res = nil
	return
}

func (javascriptTransform *JavascriptTransform) NewTransaction() *server.Transaction {
	txn := &server.Transaction{}
	txn.DatasetEntities = make(map[string][]*server.Entity)
	return txn
}

func (javascriptTransform *JavascriptTransform) ExecuteTransaction(txn *server.Transaction) error {
	return javascriptTransform.Store.ExecuteTransaction(txn)
}

func (javascriptTransform *JavascriptTransform) Log(thing interface{}, logLevel string) {
	switch strings.ToLower(logLevel) {
	case "info":
		javascriptTransform.Logger.Info(thing)
	case "warn", "warning":
		javascriptTransform.Logger.WithOptions(zap.AddStacktrace(zap.DPanicLevel)).Warn(thing)
	case "error", "err":
		javascriptTransform.Logger.WithOptions(zap.AddStacktrace(zap.DPanicLevel)).Error(thing)
	default:
		javascriptTransform.Logger.Info(thing)
	}
}

func (javascriptTransform *JavascriptTransform) Timing(name string, end bool) {
	if end {
		if _, ok := javascriptTransform.timings[name]; ok {
			timing := time.Since(javascriptTransform.timings[name])
			_ = javascriptTransform.statsDClient.Timing("transform.timing."+name,
				timing, javascriptTransform.statsDTags, 1)
		}
	} else {
		javascriptTransform.timings[name] = time.Now()
	}
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

func (javascriptTransform *JavascriptTransform) UUID() string {
	uid, _ := uuid.NewV4()
	return fmt.Sprintf("%s", uid)
}

func (javascriptTransform *JavascriptTransform) GetNamespacePrefix(urlExpansion string) string {
	ts := time.Now()

	prefix, _ := javascriptTransform.Store.NamespaceManager.GetPrefixMappingForExpansion(urlExpansion)
	_ = javascriptTransform.statsDClient.Timing("transform.GetNamespacePrefix.time",
		time.Since(ts), javascriptTransform.statsDTags, 1)
	return prefix
}

func (javascriptTransform *JavascriptTransform) AssertNamespacePrefix(urlExpansion string) string {
	ts := time.Now()
	prefix, _ := javascriptTransform.Store.NamespaceManager.AssertPrefixMappingForExpansion(urlExpansion)
	_ = javascriptTransform.statsDClient.Timing("transform.AssertNamespacePrefix.time",
		time.Since(ts), javascriptTransform.statsDTags, 1)
	return prefix
}

func (javascriptTransform *JavascriptTransform) Query(startingEntities []string, predicate string, inverse bool, datasets []string) [][]interface{} {
	ts := time.Now()
	results, err := javascriptTransform.Store.GetManyRelatedEntities(startingEntities, predicate, inverse, datasets)
	_ = javascriptTransform.statsDClient.Timing("transform.Query.time",
		time.Since(ts), javascriptTransform.statsDTags, 1)
	if err != nil {
		return nil
	}
	return results
}

func (javascriptTransform *JavascriptTransform) ById(entityId string, datasets []string) *server.Entity {
	ts := time.Now()
	entity, err := javascriptTransform.Store.GetEntity(entityId, datasets)
	_ = javascriptTransform.statsDClient.Timing("transform.ById.time",
		time.Since(ts), javascriptTransform.statsDTags, 1)
	if err != nil {
		return nil
	}
	return entity
}

func (javascriptTransform *JavascriptTransform) ToString(obj interface{}) string {
	if obj == nil {
		return "undefined"
	}

	switch obj.(type) {
	case *server.Entity:
		return fmt.Sprintf("%v", obj)
	case map[string]interface{}:
		return fmt.Sprintf("%v", obj)
	case int, int32, int64:
		return fmt.Sprintf("%d", obj)
	case float32, float64:
		return fmt.Sprintf("%g", obj)
	case bool:
		return fmt.Sprintf("%v", obj)
	default:
		return fmt.Sprintf("%s", obj)
	}
}

type QueryResultWriter interface {
	WriteObject(object any) error
	Close() error
}

func (javascriptTransform *JavascriptTransform) ExecuteQuery(resultWriter QueryResultWriter) (er error) {
	// set the passed in result writer. This is delayed in cases where the query object may exist and is bound
	// to a result writer nearer the time of execution.
	javascriptTransform.QueryResultWriter = resultWriter
	defer func(resultWriter QueryResultWriter) {
		er = resultWriter.Close()
	}(resultWriter)

	var queryFunc func() error
	err := javascriptTransform.Runtime.ExportTo(javascriptTransform.Runtime.Get("do_query"), &queryFunc)

	if err != nil {
		return err
	}

	// invoke transform, and catch js runtime err
	err = queryFunc()
	if err != nil {
		return err
	}

	return nil
}

func (javascriptTransform *JavascriptTransform) transformEntities(runner *Runner, entities []*server.Entity, jobTag string) ([]*server.Entity, error) {

	var transformFunc func(entities []*server.Entity) (interface{}, error)
	err := javascriptTransform.Runtime.ExportTo(javascriptTransform.Runtime.Get("transform_entities"), &transformFunc)
	if err != nil {
		return nil, err
	}
	javascriptTransform.statsDClient = runner.statsdClient
	javascriptTransform.statsDTags = []string{"application:datahub", "job:" + jobTag}
	javascriptTransform.timings = map[string]time.Time{}

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
			if entity, ok := e.(*server.Entity); ok {
				typeFix(entity)
				resultEntities = append(resultEntities, entity)
			} else {
				return nil, fmt.Errorf("transform emitted invalid entity: %v", e)
			}
		}
	case []*server.Entity:
		for _, entity := range v {
			typeFix(entity)
		}

		resultEntities = v
	default:
		return nil, errors.New("bad result from transform")
	}

	return resultEntities, nil
}

// if a number property is set from javascript, goja's js->golang bridge updates
// the go entity instance with int64 if the number has no decimals.
//
// in json deserialized entities, all numbers are float64.
//
// so to make goja modified entities comparable to entities produced by
// json-deserialization, we need to fix all numbers to float64.
func typeFix(entity *server.Entity) {
	for k, v := range entity.Properties {
		if i, ok := v.(int64); ok {
			entity.Properties[k] = float64(i)
		} else if i, ok := v.([]interface{}); ok {
			for c, val := range i {
				if i2, ok2 := val.(int64); ok2 {
					i[c] = float64(i2)
				}
			}
		} else if i, ok := v.(*server.Entity); ok {
			typeFix(i)
		}
	}
}

func (javascriptTransform *JavascriptTransform) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "JavascriptTransform"
	config["Code"] = base64.StdEncoding.EncodeToString(javascriptTransform.Code)
	return config
}

type HttpTransform struct {
	Url            string
	Authentication string  // "none, basic, token"
	User           string  // for use in basic auth
	Password       string  // for use in basic auth
	TokenProvider  string  // for use in token auth
	TimeOut        float64 // set timeout for http-transform
}

func (httpTransform *HttpTransform) getParallelism() int {
	return 1
}

func (httpTransform *HttpTransform) transformEntities(runner *Runner, entities []*server.Entity, jobTag string) ([]*server.Entity, error) {

	timeout := time.Duration(httpTransform.TimeOut) * time.Second
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	// create headers if needed
	url := httpTransform.Url

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
		if provider, ok := runner.tokenProviders.Get(strings.ToLower(httpTransform.TokenProvider)); ok {
			provider.Authorize(req)
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
	config["Url"] = httpTransform.Url
	config["TokenProvider"] = httpTransform.TokenProvider
	config["Authentication"] = httpTransform.Authentication
	config["TimeOut"] = httpTransform.TimeOut
	return config
}
