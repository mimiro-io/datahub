package source

import (
	"context"
	"errors"
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

type HttpDatasetSource struct {
	Endpoint       string
	Authentication string                 // "none, basic, token"
	User           string                 // for use in basic auth
	Password       string                 // for use in basic auth
	TokenProvider  security.TokenProvider // for use in token auth
	Store          *server.Store
	Logger         *zap.SugaredLogger
}

func (httpDatasetSource *HttpDatasetSource) StartFullSync() {
	// empty for now (this makes sonar not complain)
}

func (httpDatasetSource *HttpDatasetSource) EndFullSync() {
	// empty for now (this makes sonar not complain)
}

func (httpDatasetSource *HttpDatasetSource) ReadEntities(since DatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error) error {
	// open connection
	//timeout := 60 * time.Minute
	//client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

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
	if httpDatasetSource.TokenProvider != nil {
		bearer, err := httpDatasetSource.TokenProvider.Token()
		if err != nil {
			httpDatasetSource.Logger.Warnf("Token provider returned error: %w", err)
		}
		req.Header.Add("Authorization", bearer)
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
	continuationToken := &StringDatasetContinuation{}
	esp := server.NewEntityStreamParser(httpDatasetSource.Store)
	err = esp.ParseStream(res.Body, func(entity *server.Entity) error {
		if entity.ID == "@continuation" {
			continuationToken.token = entity.GetStringProperty("token")
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

func (httpDatasetSource *HttpDatasetSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "HttpDatasetSource"
	config["Url"] = httpDatasetSource.Endpoint
	config["TokenProvider"] = httpDatasetSource.TokenProvider
	return config
}

func handleHttpError(response *http.Response) error {
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.New(fmt.Sprintf("Received http source error (%d). Unable to read error body", response.StatusCode))
	}
	return errors.New(fmt.Sprintf("Received http source error (%d): %s", response.StatusCode, string(bodyBytes)))
}
