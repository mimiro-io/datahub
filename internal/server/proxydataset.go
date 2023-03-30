// Copyright 2022 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"time"
)

type ProxyDataset struct {
	badgerDataset *Dataset
	*ProxyDatasetConfig
	RemoteChangesUrl  string
	RemoteEntitiesUrl string
	auth              func(req *http.Request)
}

func (ds *Dataset) IsProxy() bool {
	return ds.ProxyConfig != nil && ds.ProxyConfig.RemoteUrl != ""
}

func (ds *Dataset) AsProxy(auth func(req *http.Request)) *ProxyDataset {
	res := &ProxyDataset{badgerDataset: ds, ProxyDatasetConfig: ds.ProxyConfig}
	res.RemoteChangesUrl, _ = UrlJoin(ds.ProxyConfig.RemoteUrl, "/changes")
	res.RemoteEntitiesUrl, _ = UrlJoin(ds.ProxyConfig.RemoteUrl, "/entities")
	res.auth = auth
	return res
}

func UrlJoin(baseUrl string, elem ...string) (result string, err error) {
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

func (d *ProxyDataset) StreamEntitiesRaw(from string, limit int, f func(jsonData []byte) error, preStream func() error) (string, error) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fullUri, nil)
	if err != nil {
		return "", err
	}
	d.auth(req)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	if res.StatusCode != 200 {
		return "", errors.New("Proxy target responded with status " + res.Status)
	}

	if preStream != nil {
		err = preStream()
		if err != nil {
			return "", err
		}
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

func (d *ProxyDataset) StreamEntities(from string, limit int, f func(*Entity) error, preStream func() error) (string, error) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fullUri, nil)
	if err != nil {
		return "", err
	}
	d.auth(req)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	if res.StatusCode != 200 {
		return "", errors.New("Proxy target responded with status " + res.Status)
	}

	if preStream != nil {
		err = preStream()
		if err != nil {
			return "", err
		}
	}

	p := NewEntityStreamParser(d.badgerDataset.store)
	var cont *Entity
	err = p.ParseStream(res.Body, func(entity *Entity) error {
		if entity.ID == "@continuation" {
			cont = entity
			return nil
		} else {
			return f(entity)
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

// StreamChangesRaw stream through the dataset's changes and call `f` for each entity.
// a `preStream` function can be provided if StreamChangesRaw is used in a web handler. It allows
// to leave the http response uncommitted until `f` is called, so that an http error handler
// still can modify status code while the response is uncommitted
func (d *ProxyDataset) StreamChangesRaw(since string, limit int, latestOnly bool, reverse bool, f func(jsonData []byte) error, preStream func()) (string, error) {
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
	if reverse {
		q.Add("reverse", "true")
	}
	if latestOnly {
		q.Add("latestOnly", "true")
	}

	uri.RawQuery = q.Encode()
	fullUri := uri.String()
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fullUri, nil)
	if err != nil {
		return "", err
	}
	d.auth(req)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	if res.StatusCode != 200 {
		return "", errors.New("Proxy target responded with status " + res.Status)
	}

	if preStream != nil {
		preStream()
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

// StreamChangesRaw stream through the dataset's changes and call `f` for each entity.
// a `preStream` function can be provided if StreamChanges is used in a web handler. It allows
// to leave the http response uncommitted until `f` is called, so that an http error handler
// still can modify status code while the response is uncommitted
func (d *ProxyDataset) StreamChanges(since string, limit int, latestOnly bool, reverse bool, f func(*Entity) error, preStream func()) (string, error) {
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
	if reverse {
		q.Add("reverse", "true")
	}
	if latestOnly {
		q.Add("latestOnly", "true")
	}

	uri.RawQuery = q.Encode()
	fullUri := uri.String()
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fullUri, nil)
	if err != nil {
		return "", err
	}
	d.auth(req)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	if res.StatusCode != 200 {
		return "", errors.New("Proxy target responded with status " + res.Status)
	}

	if preStream != nil {
		preStream()
	}
	p := NewEntityStreamParser(d.badgerDataset.store)
	var cont *Entity
	err = p.ParseStream(res.Body, func(entity *Entity) error {
		if entity.ID == "@continuation" {
			cont = entity
			return nil
		} else {
			return f(entity)
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

func (d *ProxyDataset) ForwardEntities(sourceBody io.ReadCloser, sourceHeader http.Header) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", d.RemoteEntitiesUrl, sourceBody)
	for k, v := range sourceHeader {
		if strings.HasPrefix(strings.ToLower(k), "universal-data-api") {
			for _, val := range v {
				req.Header.Add(k, val)
			}
		}
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return errors.New("Proxy target responded with status " + res.Status)
	}
	return nil
}
