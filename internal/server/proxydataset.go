// Copyright 2022 MIMIRO AS
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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"
)

type ProxyDataset struct {
	badgerDataset *Dataset
	*proxyDatasetConfig
	RemoteChangesUrl  string
	RemoteEntitiesUrl string
}

func (ds *Dataset) IsProxy() bool {
	return ds.ProxyConfig != nil && ds.ProxyConfig.RemoteUrl != ""
}

func (ds *Dataset) AsProxy() *ProxyDataset {
	res := &ProxyDataset{badgerDataset: ds, proxyDatasetConfig: ds.ProxyConfig}
	res.RemoteChangesUrl, _ = urlJoin(ds.ProxyConfig.RemoteUrl, "/changes")
	res.RemoteEntitiesUrl, _ = urlJoin(ds.ProxyConfig.RemoteUrl, "/entities")
	return res
}
func urlJoin(baseUrl string, elem ...string) (result string, err error) {
	u, err := url.Parse(baseUrl)
	if err != nil {
		return
	}
	if len(elem) > 0 {
		elem = append([]string{u.Path}, elem...)
		u.Path = path.Join(elem...)
	}
	result = u.String()
	return
}

func (d *ProxyDataset) StreamEntitiesRaw(from string, limit int, f func(jsonData []byte) error) (string, error) {
	uri, err := url.Parse(d.RemoteEntitiesUrl)
	if err != nil {
		return "", err
	}
	q := uri.Query()
	if from != "" {
		q.Add("from", from)
	}
	if limit > 0 {
		q.Add("limit", strconv.Itoa(limit))
	}
	uri.RawQuery = q.Encode()
	fullUri := uri.String()
	res, err := http.Get(fullUri)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 200 {
		return "", errors.New("Proxy target responded with status " + res.Status)
	}
	p := NewEntityStreamParser(d.badgerDataset.store)
	var cont *Entity
	err = p.ParseStream(res.Body, func(entity *Entity) error {
		if entity.ID == "@continuation" {
			cont = entity
			return nil
		} else {
			jsonEntity, err2 := json.Marshal(entity)
			if err2 != nil {
				return err2
			}
			return f(jsonEntity)
		}
	})
	if err != nil {
		return "", err
	}

	if cont == nil {
		return "", nil
	}
	return cont.Properties["token"].(string), nil

}
func (d *ProxyDataset) StreamChangesRaw(since string, limit int, f func(jsonData []byte) error) (string, error) {
	uri, err := url.Parse(d.RemoteChangesUrl)
	if err != nil {
		return "", err
	}
	q := uri.Query()
	if since != "" {
		q.Add("since", since)
	}
	if limit > 0 {
		q.Add("limit", strconv.Itoa(limit))
	}
	uri.RawQuery = q.Encode()
	fullUri := uri.String()
	res, err := http.Get(fullUri)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 200 {
		return "", errors.New("Proxy target responded with status " + res.Status)
	}
	p := NewEntityStreamParser(d.badgerDataset.store)
	var cont *Entity
	err = p.ParseStream(res.Body, func(entity *Entity) error {
		if entity.ID == "@continuation" {
			cont = entity
			return nil
		} else {
			jsonEntity, err2 := json.Marshal(entity)
			if err2 != nil {
				return err2
			}
			return f(jsonEntity)
		}
	})
	if err != nil {
		return "", err
	}

	if cont == nil {
		return "", nil
	}
	return cont.Properties["token"].(string), nil

}

func (d *ProxyDataset) ForwardEntities(body io.ReadCloser) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", d.RemoteEntitiesUrl, body)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return errors.New("Proxy target responded with status " + res.Status)
	}
	return nil
}
