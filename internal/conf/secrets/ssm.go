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

package secrets

import (
	"time"

	awsssm "github.com/PaddleHQ/go-aws-ssm"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"go.uber.org/zap"
)

type SsmManagerConfig struct {
	Env         string
	Key         string
	Logger      *zap.SugaredLogger
	cache       *map[string]interface{}
	lastUpdated time.Time
}

type ssmProps interface {
	HasKey(key string) bool
	Value(key string) (string, bool)
}

/*
Contains a ssm backed Key/value properties store.
The store will refresh at intervals
*/
type SsmProperties struct {
	config *SsmManagerConfig
}

const localEndpoint = "http://localhost:4566"

// NewSsm is a constructor used to create an SsmProperties
// if an error occurs in the creation, that is returned instead
// and should be handled accordingly
//
// It will attempt to load the Key string given from ssm, and it
// will map all values to a map
func NewSsm(config *SsmManagerConfig) (*SsmProperties, error) {

	params, err := config.loadParams()
	if err != nil {
		return nil, err
	}
	config.cache = params
	config.lastUpdated = time.Now()
	return &SsmProperties{config: config}, nil
}

// HasKey validates if the Key given exists in the cache
// Will return false if the cache is nil
func (p *SsmProperties) HasKey(key string) bool {
	if p.config.cache == nil {
		return false
	}
	c := *p.config.cache
	_, ok := c[key]
	return ok
}

// Value returns the (value, true) if found, or ("", false) if not
// It performs a nil check against the cache
func (p *SsmProperties) Value(key string) (string, bool) {
	if p.config.cache == nil {
		return "", false
	}
	c := *p.config.cache
	val, ok := c[key]
	return val.(string), ok
}

// Params returns the full cached set of parameters as a map.
// A nil check should be performed against the returned value
func (p *SsmProperties) Params() *map[string]interface{} {
	if p == nil || p.config == nil || p.config.cache == nil { // this happens in test environments
		return &map[string]interface{}{}
	}
	return p.config.cache
}

func (c *SsmManagerConfig) loadParams() (*map[string]interface{}, error) {
	store, err := c.getClient()
	if err != nil {
		return nil, err
	}

	params, err := store.GetAllParametersByPath(c.Key, true)
	if err != nil {
		return nil, err
	}

	out := &map[string]interface{}{}
	err = params.Decode(out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *SsmManagerConfig) getClient() (*awsssm.ParameterStore, error) {
	if c.Env == "local" {
		c.Logger.Info("Loading ssm with localstack")
		s, err := session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("foo", "var", ""),
			Region:      aws.String(endpoints.UsEast1RegionID),
			Endpoint:    aws.String(localEndpoint),
		})
		if err != nil {
			return nil, err
		}

		return awsssm.NewParameterStoreWithClient(ssm.New(s)), nil
	} else {
		store, err := awsssm.NewParameterStore(&aws.Config{
			Region: aws.String("eu-west-1"),
		})

		if err != nil {
			return nil, err
		}
		return store, nil
	}
}
