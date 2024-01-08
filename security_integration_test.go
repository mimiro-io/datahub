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

package datahub_test

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"github.com/mimiro-io/datahub"
	"github.com/mimiro-io/datahub/internal/conf"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/mimiro-io/datahub/internal/security"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFromOutside(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

// privateKey and well-known.json are generated from https://mkjwk.org/
var privateKey = `-----BEGIN PRIVATE KEY-----
MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAUAwggE8AgEAAkEAgp2HWNZwdVzEflWx
jK8hddWr2x+IKazSpMMfLg8oDQk+kYI6/ChNS4mdHWD58tQzI1FimW5z1lfPoSvc
I5LzCwIDAQABAkBABnH7BRqZHQEgoGbo/EvdlACq57j6HMIgi5j0He/W+1SbAsoc
zaAK2Wgr10dOt+r8URQ1BzYHokap67oLmy9RAiEA1eNGzCLrJsLO3OSaDsmBM0BQ
Zks10U7AEugv+mPYuHMCIQCcVQR6isuehozn4YGev3jOZe6QuZUfAzw8elyFRobt
CQIhALzted7dRTtCvnjt0IsZQO+lcp849fvBhPXudFrHEWqzAiEAjVvi3Nu8GvAX
YWVry5vfJOLOwVbOHGjUgusx1eFcB+ECIQCyMIG0HM3l+maWePciN+ucgAhNLiiY
9LvRBDUAB4Eoqw==
-----END PRIVATE KEY-----`

//	var publicKey = `-----BEGIN PUBLIC KEY-----
//MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAIKdh1jWcHVcxH5VsYyvIXXVq9sfiCms
//0qTDHy4PKA0JPpGCOvwoTUuJnR1g+fLUMyNRYpluc9ZXz6Er3COS8wsCAwEAAQ==
//-----END PUBLIC KEY-----`

var wellKnown = `{"keys":[{
    "kty": "RSA",
    "e": "AQAB",
    "use": "sig",
    "kid": "letmein",
    "alg": "RS256",
    "n": "gp2HWNZwdVzEflWxjK8hddWr2x-IKazSpMMfLg8oDQk-kYI6_ChNS4mdHWD58tQzI1FimW5z1lfPoSvcI5LzCw"
}]}`

// to do negative testing
var invalidPrivateKey = `-----BEGIN PRIVATE KEY-----
MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAghnDh8yBi6fUe0aZ
WDwF/PQUsya9IcuUmRmLbDiNTg/RyUOwrD9jn7cgK3KmOVkZExH3p0naYtfZWAA3
T74leQIDAQABAkBtkHTysG+IxVZUWyhY/nOsi4HDPiSZiXEjXjfkc9vMmKKYD5i4
Uofi1kzfVJUepsAKsQTjz/Fb6Htv7cnfA1VhAiEAt0wwZkco0o4GtbM0YkXPJWVV
TUrlcUCX8bJR8cRNajMCIQC1tAU5NvyER4offN/00xbtQKNSzbGuIna29gxnQKVd
owIgZbMGH5lX0wsuOd5GMEhfWHZSz+HAUAfwxg+UW7n44fUCIQCn6DfffDlyv20a
1dlasun/yfhi3yNvB/ft8IbbrzhgwwIgZS6QVrn9Ro/oEnrTM0FuIigl1sXlZT/w
yLOZNSc/qIQ=
-----END PRIVATE KEY-----`

var _ = Describe("The dataset endpoint", Ordered, func() {
	var app *datahub.DatahubInstance

	location := "./node_security_integration_test"

	securityLocation := "./node_security_integration_test_clients"
	datahubURL := "http://localhost:24998/"
	var wellKnownServer *http.Server
	BeforeAll(func() {
		_ = os.RemoveAll(location)
		_ = os.RemoveAll(securityLocation)
		_ = os.Setenv("STORE_LOCATION", location)
		_ = os.Setenv("PROFILE", "test")
		_ = os.Setenv("SERVER_PORT", "24998")

		_ = os.Setenv("AUTHORIZATION_MIDDLEWARE", "local")
		_ = os.Setenv("ADMIN_USERNAME", "admin")
		_ = os.Setenv("ADMIN_PASSWORD", "admin")
		//_ = os.Setenv("ENABLE_NODE_SECURITY", "true")
		_ = os.Setenv("NODE_ID", "node1")
		_ = os.Setenv("TOKEN_AUDIENCE", "test_audience")
		_ = os.Setenv("TOKEN_ISSUER", "test_issuer")
		_ = os.Setenv("SECURITY_STORAGE_LOCATION", securityLocation)
		_ = os.Setenv("DL_JWT_CLIENT_ID", "dummy_provider")

		_ = os.Setenv("TOKEN_WELL_KNOWN", "http://localhost:14447/well-known.json")

		oldOut := os.Stdout
		oldErr := os.Stderr
		devNull, _ := os.Open("/dev/null")
		os.Stdout = devNull
		os.Stderr = devNull
		// app, _ = datahub.Start(context.Background())

		config, _ := conf.LoadConfig("")
		app, _ = datahub.NewDatahubInstance(config)
		go app.Start()

		os.Stdout = oldOut
		os.Stderr = oldErr

		// start an external token validation endpoint
		os.WriteFile(location+"/well-known.json", []byte(wellKnown), 0644)

		mux := http.NewServeMux()
		mux.Handle("/", http.FileServer(http.Dir(location)))
		wellKnownServer = &http.Server{Addr: ":14447", Handler: mux}
		go wellKnownServer.ListenAndServe()
	})
	AfterAll(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		err := wellKnownServer.Shutdown(ctx)
		Expect(err).To(BeNil())
		err = app.Stop(ctx)
		defer cancel()
		Expect(err).To(BeNil())
		err = os.RemoveAll(location)
		Expect(err).To(BeNil())
		err = os.RemoveAll(securityLocation)
		Expect(err).To(BeNil())
		_ = os.Unsetenv("STORE_LOCATION")
		_ = os.Unsetenv("PROFILE")
		_ = os.Unsetenv("SERVER_PORT")

		_ = os.Unsetenv("AUTHORIZATION_MIDDLEWARE")
		_ = os.Unsetenv("ADMIN_USERNAME")
		_ = os.Unsetenv("ADMIN_PASSWORD")
		_ = os.Unsetenv("ENABLE_NODE_SECURITY")
		_ = os.Unsetenv("NODE_ID")
		_ = os.Unsetenv("SECURITY_STORAGE_LOCATION")
		_ = os.Unsetenv("TOKEN_WELL_KNOWN")
	})

	It("Should authenticate with admin credentials", func(_ SpecContext) {
		token := getAdminToken(datahubURL, "admin", "admin")

		// check JWT valid for endpoint access (also tests the admin role)
		reqURL := datahubURL + "datasets"
		client := &http.Client{}
		req, _ := http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err := client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))
	}, SpecTimeout(15*time.Minute))

	It("Should support admin control over data structures", func(_ SpecContext) {
		token := getAdminToken(datahubURL, "admin", "admin")
		// check JWT valid for endpoint access (also tests the admin role)
		reqURL := datahubURL + "datasets"
		client := &http.Client{}
		req, _ := http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err := client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// register new client
		clientInfo := &security.ClientInfo{}
		clientInfo.ClientID = "client1"
		_, publicKey := security.GenerateRsaKeyPair()
		publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
		Expect(err).To(BeNil())
		clientInfo.PublicKey = []byte(publicKeyPem)

		clientJSON, err := json.Marshal(clientInfo)
		Expect(err).To(BeNil())

		reqURL = datahubURL + "security/clients"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// get list of registered clients
		reqURL = datahubURL + "security/clients"
		client = &http.Client{}
		req, _ = http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))
		clientData, err := io.ReadAll(res.Body)
		Expect(err).To(BeNil())
		var clients map[string]*security.ClientInfo
		err = json.Unmarshal(clientData, &clients)
		Expect(err).To(BeNil())

		Expect(len(clients)).To(Equal(1))
		Expect(clients["client1"].PublicKey).To(Equal([]byte(publicKeyPem)))

		// delete client
		clientInfo = &security.ClientInfo{}
		clientInfo.ClientID = "client1"
		clientInfo.Deleted = true
		clientJSON, err = json.Marshal(clientInfo)
		Expect(err).To(BeNil())

		reqURL = datahubURL + "security/clients"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// get clients and check client1 is not there
		reqURL = datahubURL + "security/clients"
		client = &http.Client{}
		req, _ = http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}
		res, err = client.Do(req)
		Expect(err).To(BeNil())
		clientData, err = io.ReadAll(res.Body)
		Expect(err).To(BeNil())
		// cd := string(clientData)
		// cd = cd + ""
		var deletedClients map[string]*security.ClientInfo
		err = json.Unmarshal(clientData, &deletedClients)
		Expect(err).To(BeNil())
		Expect(len(deletedClients)).To(Equal(0))

		// register client acls
		acls := make([]*security.AccessControl, 0)
		acl1 := &security.AccessControl{}
		acl1.Action = "read"
		acl1.Resource = "/datasets"
		acls = append(acls, acl1)
		aclJSON, err := json.Marshal(acls)
		Expect(err).To(BeNil())

		reqURL = datahubURL + "security/clients/client1/acl"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// fetch client acls
		reqURL = datahubURL + "security/clients/client1/acl"
		client = &http.Client{}
		req, _ = http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}
		res, err = client.Do(req)
		Expect(err).To(BeNil())
		clientData, err = io.ReadAll(res.Body)
		Expect(err).To(BeNil())
		var clientacls []*security.AccessControl
		err = json.Unmarshal(clientData, &clientacls)
		Expect(err).To(BeNil())
		Expect(len(clientacls)).To(Equal(1))

		// clear client acls
		reqURL = datahubURL + "security/clients/client1/acl"
		client = &http.Client{}
		req, _ = http.NewRequest("DELETE", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// register client roles
		reqURL = datahubURL + "security/clients/client1/acl"
		client = &http.Client{}
		req, _ = http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}
		res, err = client.Do(req)
		Expect(err).To(BeNil())
		clientData, err = io.ReadAll(res.Body)
		Expect(err).To(BeNil())
		var deletedeclientacls []*security.AccessControl
		err = json.Unmarshal(clientData, &deletedeclientacls)
		Expect(err).To(BeNil())
		Expect(len(deletedeclientacls)).To(Equal(0))
	}, SpecTimeout(15*time.Minute))
	It("Should register client and allow access", func(_ SpecContext) {
		token := getAdminToken(datahubURL, "admin", "admin")
		// check JWT valid for endpoint access (also tests the admin role)

		clientPrivateKey := registerNodeSecClient(datahubURL, token, "client1")

		// register client acls
		acls := make([]*security.AccessControl, 0)
		acl1 := &security.AccessControl{}
		acl1.Action = "write"
		acl1.Resource = "/datasets*"
		acls = append(acls, acl1)
		aclJSON, err := json.Marshal(acls)
		Expect(err).To(BeNil())

		reqURL := datahubURL + "security/clients/client1/acl"
		client := &http.Client{}
		req, _ := http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err := client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// get jwt for client
		clientToken := createNodeSecToken(datahubURL, "client1", "node1", clientPrivateKey)

		// use token to list datasets
		reqURL = datahubURL + "datasets"
		client = &http.Client{}
		req, _ = http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + clientToken},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		jsonraw, _ := io.ReadAll(res.Body)
		result := make([]map[string]interface{}, 0)
		err = json.Unmarshal(jsonraw, &result)
		Expect(err).To(BeNil())
		Expect(len(result) == 1)
		Expect(result[0]["Name"]).To(Equal("core.Dataset"))

		// use token to try to list jobs
		reqURL = datahubURL + "jobs"
		client = &http.Client{}
		req, _ = http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + clientToken},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(403))

		jsonraw, _ = io.ReadAll(res.Body)
		Expect(string(jsonraw)).To(Equal("{\"message\":\"user does not have permission\"}\n"))
	}, SpecTimeout(15*time.Minute))

	It("Should allow access to self via job and node jwt provider", func(_ SpecContext) {
		token := getAdminToken(datahubURL, "admin", "admin")
		// check JWT valid for endpoint access (also tests the admin role)
		reqURL := datahubURL + "datasets"
		client := &http.Client{}
		req, _ := http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err := client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// register this node as a client to itself
		clientInfo := &security.ClientInfo{}
		clientInfo.ClientID = "node1"

		// read the node private and public keys for use in this interaction
		_, err = os.ReadFile(securityLocation + string(os.PathSeparator) + "node_key")
		Expect(err).To(BeNil())
		// clientPrivateKey, err := security.ParseRsaPrivateKeyFromPem(contentService)

		content, err := os.ReadFile(securityLocation + string(os.PathSeparator) + "node_key.pub")
		Expect(err).To(BeNil())
		publicKey, err := security.ParseRsaPublicKeyFromPem(content)
		Expect(err).To(BeNil())

		// clientPrivateKey, publicKey := security.GenerateRsaKeyPair()
		publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
		Expect(err).To(BeNil())
		clientInfo.PublicKey = []byte(publicKeyPem)

		clientJSON, err := json.Marshal(clientInfo)
		Expect(err).To(BeNil())

		reqURL = datahubURL + "security/clients"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// register acl for client
		acls := make([]*security.AccessControl, 0)
		acl1 := &security.AccessControl{}
		acl1.Action = "write"
		acl1.Resource = "/datasets*"
		acls = append(acls, acl1)
		aclJSON, err := json.Marshal(acls)
		Expect(err).To(BeNil())

		reqURL = datahubURL + "security/clients/node1/acl"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// register nodetokenprovider
		providerConfig := &security.ProviderConfig{}
		providerConfig.Name = "node1provider"
		providerConfig.Type = "nodebearer"
		providerConfig.Endpoint = &security.ValueReader{
			Type:  "text",
			Value: datahubURL + "security/token",
		}
		providerConfig.Audience = &security.ValueReader{
			Type:  "text",
			Value: "node1",
		}

		providerJSON, err := json.Marshal(providerConfig)
		Expect(err).To(BeNil())
		reqURL = datahubURL + "provider/logins"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(providerJSON))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		// upload job to access remote (loopback) dataset
		jobJSON := `{
			"id" : "sync-from-remote-dataset-with-node-provider",
			"title" : "sync-from-remote-dataset-with-node-provider",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "` + datahubURL + `datasets/core.Dataset/changes",
				"TokenProvider" : "node1provider"
			},
			"sink" : {
				"Type" : "DevNullSink"
			}}`

		reqURL = datahubURL + "jobs"
		client = &http.Client{}
		req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer([]byte(jobJSON)))
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}

		res, err = client.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(201))

		// run job
		req, _ = http.NewRequest("PUT", datahubURL+"job/sync-from-remote-dataset-with-node-provider/run", nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + token},
		}
		res, err = http.DefaultClient.Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		for {
			time.Sleep(100 * time.Millisecond)
			// check job status
			req, _ = http.NewRequest("GET", datahubURL+"jobs/_/history", nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeNil())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var hist []map[string]interface{}
			_ = json.Unmarshal(body, &hist)
			// t.Log(hist)
			if len(hist) != 0 {
				Expect(hist[0]["lastError"]).To(BeZero(), "no error expected")
				break
			}

		}
	}, SpecTimeout(15*time.Minute))

	It("Should support access via external jwt validator", func() {
		// give "bob" access to datasets
		giveBobACLForPaths(datahubURL, "admin", "admin", "/datasets*")

		// make an external JWT for bob
		externalToken, err := createOauthJwtToken(privateKey, "bob", "test_issuer", "test_audience")
		Expect(err).To(BeNil())
		// use token to list datasets
		reqURL := datahubURL + "datasets"
		req, _ := http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + externalToken},
		}

		res, err := (&http.Client{}).Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(200))

		jsonraw, _ := io.ReadAll(res.Body)
		result := make([]map[string]interface{}, 0)
		err = json.Unmarshal(jsonraw, &result)
		Expect(err).To(BeNil())
		Expect(len(result) == 1)
		Expect(result[0]["Name"]).To(Equal("core.Dataset"))
	})

	It("Should deny access via external jwt validator if invalid token", func() {
		giveBobACLForPaths(datahubURL, "admin", "admin", "/datasets*")

		externalToken, err := createOauthJwtToken(invalidPrivateKey, "bob", "test_issuer", "test_audience")
		Expect(err).To(BeNil())

		// use token to list datasets
		reqURL := datahubURL + "datasets"
		req, _ := http.NewRequest("GET", reqURL, nil)
		req.Header = http.Header{
			"Content-Type":  []string{"application/json"},
			"Authorization": []string{"Bearer " + externalToken},
		}

		res, err := (&http.Client{}).Do(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.StatusCode).To(Equal(401))

		jsonraw, _ := io.ReadAll(res.Body)
		Expect(string(jsonraw)).To(Equal("{\"message\":\"Oauth: crypto/rsa: verification error\"}\n"))
	})

	/*
		It("Should remove client access", func() {
			// create new dataset
			res, err := http.Post(datahubURL, "application/json", strings.NewReader(""))
			Expect(err).To(BeNil(),)
			Expect(res).NotTo(BeZero(),)
			Expect(res.StatusCode).To(Equal(200))
		})

		It("Should retain users and acls after restart", func() {
			// create new dataset
			res, err := http.Post(datahubURL, "application/json", strings.NewReader(""))
			Expect(err).To(BeNil(),)
			Expect(res).NotTo(BeZero(),)
			Expect(res.StatusCode).To(Equal(200))
		})

		It("allow roles to be allocated to a client", func() {
			// create new dataset
			res, err := http.Post(datahubURL, "application/json", strings.NewReader(""))
			Expect(err).To(BeNil(),)
			Expect(res).NotTo(BeZero(),)
			Expect(res.StatusCode).To(Equal(200))
		})
	*/
})

func registerNodeSecClient(datahubURL string, adminToken string, subject string) *rsa.PrivateKey {
	// register new client
	clientInfo := &security.ClientInfo{}
	clientInfo.ClientID = subject
	clientPrivateKey, publicKey := security.GenerateRsaKeyPair()
	publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
	Expect(err).To(BeNil())
	clientInfo.PublicKey = []byte(publicKeyPem)

	clientJSON, err := json.Marshal(clientInfo)
	Expect(err).To(BeNil())

	reqURL := datahubURL + "security/clients"
	client := &http.Client{}
	req, _ := http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
	req.Header = http.Header{
		"Content-Type":  []string{"application/json"},
		"Authorization": []string{"Bearer " + adminToken},
	}

	res, err := client.Do(req)
	Expect(err).To(BeNil())
	Expect(res).NotTo(BeNil())
	Expect(res.StatusCode).To(Equal(200))

	return clientPrivateKey
}

func createNodeSecToken(datahubURL string, subject string, audience string, clientPrivateKey *rsa.PrivateKey) string {
	GinkgoHelper()
	data1 := url.Values{}
	data1.Set("grant_type", "client_credentials")
	data1.Set("client_assertion_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")

	pem, err := security.CreateJWTForTokenRequest(subject, audience, clientPrivateKey)
	Expect(err).To(BeNil())
	data1.Set("client_assertion", pem)

	reqURL := datahubURL + "security/token"
	res, err := http.PostForm(reqURL, data1)
	Expect(err).To(BeNil())
	Expect(res).NotTo(BeZero())
	Expect(res.StatusCode).To(Equal(200))

	decoder := json.NewDecoder(res.Body)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	Expect(err).To(BeNil())
	clientToken := response["access_token"].(string)
	Expect(clientToken).NotTo(BeNil())
	return clientToken
}

func giveBobACLForPaths(datahubURL string, user string, pwd string, paths ...string) {
	GinkgoHelper()
	// give "bob" access to datasets
	acls := []*security.AccessControl{}
	for _, path := range paths {
		acls = append(acls, &security.AccessControl{Action: "write", Resource: path})
	}
	aclJSON, err := json.Marshal(acls)
	Expect(err).To(BeNil())

	adminToken := getAdminToken(datahubURL, user, pwd)
	reqURL := datahubURL + "security/clients/bob/acl"
	req, _ := http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
	req.Header = http.Header{
		"Content-Type":  []string{"application/json"},
		"Authorization": []string{"Bearer " + adminToken},
	}

	res, err := http.DefaultClient.Do(req)
	Expect(err).To(BeNil())
	Expect(res).NotTo(BeNil())
	Expect(res.StatusCode).To(Equal(200))
}

func createOauthJwtToken(privateKeyPEM string, subject string, issuer string, audience string) (string, error) {
	privateKeyFromPEM, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(privateKeyPEM))

	// make an external JWT for bob
	claims := security.CustomClaims{}
	claims.Roles = []string{"client"}
	claims.RegisteredClaims = jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute * 15)),
		Issuer:    issuer,                     // must match TOKEN_ISSUER
		Audience:  jwt.ClaimStrings{audience}, //must match TOKEN_AUDIENCE
		Subject:   subject,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "letmein"
	externalToken, err := token.SignedString(privateKeyFromPEM)
	return externalToken, err
}

func getAdminToken(datahubURL string, user string, pwd string) string {
	GinkgoHelper()
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", user)
	data.Set("client_secret", pwd)

	reqURL := datahubURL + "security/token"
	res, err := http.PostForm(reqURL, data)
	Expect(err).To(BeNil())
	Expect(res).NotTo(BeZero())
	Expect(res.StatusCode).To(Equal(200))

	decoder := json.NewDecoder(res.Body)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	Expect(err).To(BeNil())
	token := response["access_token"].(string)
	Expect(token).NotTo(BeNil())
	return token
}
