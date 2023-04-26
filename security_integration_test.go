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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/franela/goblin"
	"go.uber.org/fx"

	"github.com/mimiro-io/datahub"
	"github.com/mimiro-io/datahub/internal/security"
)

func TestNodeSecurity(t *testing.T) {
	g := goblin.Goblin(t)

	var app *fx.App

	location := "./node_security_integration_test"
	securityLocation := "./node_security_integration_test_clients"
	datahubURL := "http://localhost:24998/"

	g.Describe("The dataset endpoint", func() {
		g.Before(func() {
			_ = os.RemoveAll(location)
			_ = os.RemoveAll(securityLocation)
			_ = os.Setenv("STORE_LOCATION", location)
			_ = os.Setenv("PROFILE", "test")
			_ = os.Setenv("SERVER_PORT", "24998")

			_ = os.Setenv("AUTHORIZATION_MIDDLEWARE", "local")
			_ = os.Setenv("ADMIN_USERNAME", "admin")
			_ = os.Setenv("ADMIN_PASSWORD", "admin")
			_ = os.Setenv("ENABLE_NODE_SECURITY", "true")
			_ = os.Setenv("NODE_ID", "node1")
			_ = os.Setenv("SECURITY_STORAGE_LOCATION", securityLocation)
			_ = os.Setenv("DL_JWT_CLIENT_ID", "dummy_provider")

			oldOut := os.Stdout
			oldErr := os.Stderr
			devNull, _ := os.Open("/dev/null")
			os.Stdout = devNull
			os.Stderr = devNull
			app, _ = datahub.Start(context.Background())
			os.Stdout = oldOut
			os.Stderr = oldErr
		})
		g.After(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			err := app.Stop(ctx)
			defer cancel()
			g.Assert(err).IsNil()
			err = os.RemoveAll(location)
			g.Assert(err).IsNil()
			err = os.RemoveAll(securityLocation)
			g.Assert(err).IsNil()
			_ = os.Unsetenv("STORE_LOCATION")
			_ = os.Unsetenv("PROFILE")
			_ = os.Unsetenv("SERVER_PORT")

			_ = os.Unsetenv("AUTHORIZATION_MIDDLEWARE")
			_ = os.Unsetenv("ADMIN_USERNAME")
			_ = os.Unsetenv("ADMIN_PASSWORD")
			_ = os.Unsetenv("ENABLE_NODE_SECURITY")
			_ = os.Unsetenv("NODE_ID")
			_ = os.Unsetenv("SECURITY_STORAGE_LOCATION")
		})

		g.It("Should authenticate with admin credentials", func() {
			g.Timeout(15 * time.Minute)

			// create new dataset
			data := url.Values{}
			data.Set("grant_type", "client_credentials")
			data.Set("client_id", "admin")
			data.Set("client_secret", "admin")

			reqURL := datahubURL + "security/token"
			res, err := http.PostForm(reqURL, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			g.Assert(err).IsNil()
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqURL = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)
		})

		g.It("Should support admin control over data structures", func() {
			g.Timeout(15 * time.Minute)

			// create new dataset
			data := url.Values{}
			data.Set("grant_type", "client_credentials")
			data.Set("client_id", "admin")
			data.Set("client_secret", "admin")

			reqURL := datahubURL + "security/token"
			res, err := http.PostForm(reqURL, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			g.Assert(err).IsNil()
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqURL = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register new client
			clientInfo := &security.ClientInfo{}
			clientInfo.ClientID = "client1"
			_, publicKey := security.GenerateRsaKeyPair()
			publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
			g.Assert(err).IsNil()
			clientInfo.PublicKey = []byte(publicKeyPem)

			clientJSON, err := json.Marshal(clientInfo)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// get list of registered clients
			reqURL = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)
			clientData, err := io.ReadAll(res.Body)
			g.Assert(err).IsNil()
			var clients map[string]*security.ClientInfo
			err = json.Unmarshal(clientData, &clients)
			g.Assert(err).IsNil()

			g.Assert(len(clients)).Equal(1)
			g.Assert(clients["client1"].PublicKey).Equal([]byte(publicKeyPem))

			// delete client
			clientInfo = &security.ClientInfo{}
			clientInfo.ClientID = "client1"
			clientInfo.Deleted = true
			clientJSON, err = json.Marshal(clientInfo)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// get clients and check client1 is not there
			reqURL = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = client.Do(req)
			g.Assert(err).IsNil()
			clientData, err = io.ReadAll(res.Body)
			g.Assert(err).IsNil()
			// cd := string(clientData)
			// cd = cd + ""
			var deletedClients map[string]*security.ClientInfo
			err = json.Unmarshal(clientData, &deletedClients)
			g.Assert(err).IsNil()
			g.Assert(len(deletedClients)).Equal(0)

			// register client acls
			acls := make([]*security.AccessControl, 0)
			acl1 := &security.AccessControl{}
			acl1.Action = "read"
			acl1.Resource = "/datasets"
			acls = append(acls, acl1)
			aclJSON, err := json.Marshal(acls)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// fetch client acls
			reqURL = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = client.Do(req)
			g.Assert(err).IsNil()
			clientData, err = io.ReadAll(res.Body)
			g.Assert(err).IsNil()
			var clientacls []*security.AccessControl
			err = json.Unmarshal(clientData, &clientacls)
			g.Assert(err).IsNil()
			g.Assert(len(clientacls)).Equal(1)

			// clear client acls
			reqURL = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("DELETE", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register client roles
			reqURL = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = client.Do(req)
			g.Assert(err).IsNil()
			clientData, err = io.ReadAll(res.Body)
			g.Assert(err).IsNil()
			var deletedeclientacls []*security.AccessControl
			err = json.Unmarshal(clientData, &deletedeclientacls)
			g.Assert(err).IsNil()
			g.Assert(len(deletedeclientacls)).Equal(0)
		})
		g.It("Should register client and allow access", func() {
			g.Timeout(15 * time.Minute)

			// get jwt for admin
			data := url.Values{}
			data.Set("grant_type", "client_credentials")
			data.Set("client_id", "admin")
			data.Set("client_secret", "admin")

			reqURL := datahubURL + "security/token"
			res, err := http.PostForm(reqURL, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			g.Assert(err).IsNil()
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqURL = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register new client
			clientInfo := &security.ClientInfo{}
			clientInfo.ClientID = "client1"
			clientPrivateKey, publicKey := security.GenerateRsaKeyPair()
			publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
			g.Assert(err).IsNil()
			clientInfo.PublicKey = []byte(publicKeyPem)

			clientJSON, err := json.Marshal(clientInfo)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register client acls
			acls := make([]*security.AccessControl, 0)
			acl1 := &security.AccessControl{}
			acl1.Action = "write"
			acl1.Resource = "/datasets*"
			acls = append(acls, acl1)
			aclJSON, err := json.Marshal(acls)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// get jwt for client
			data1 := url.Values{}
			data1.Set("grant_type", "client_credentials")
			data1.Set("client_assertion_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")

			pem, err := security.CreateJWTForTokenRequest("client1", "node1", clientPrivateKey)
			g.Assert(err).IsNil()
			data1.Set("client_assertion", pem)

			reqURL = datahubURL + "security/token"
			res, err = http.PostForm(reqURL, data1)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder = json.NewDecoder(res.Body)
			response = make(map[string]interface{})
			err = decoder.Decode(&response)
			g.Assert(err).IsNil()
			clientToken := response["access_token"].(string)
			g.Assert(clientToken).IsNotNil()

			// use token to list datasets
			reqURL = datahubURL + "datasets"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + clientToken},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			jsonraw, _ := io.ReadAll(res.Body)
			result := make([]map[string]interface{}, 0)
			err = json.Unmarshal(jsonraw, &result)
			g.Assert(err).IsNil()
			g.Assert(len(result) == 1)
		})

		g.It("Should allow access to self via job and node jwt provider", func() {
			g.Timeout(15 * time.Minute)

			// get jwt for admin
			data := url.Values{}
			data.Set("grant_type", "client_credentials")
			data.Set("client_id", "admin")
			data.Set("client_secret", "admin")

			reqURL := datahubURL + "security/token"
			res, err := http.PostForm(reqURL, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			g.Assert(err).IsNil()
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqURL = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqURL, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register this node as a client to itself
			clientInfo := &security.ClientInfo{}
			clientInfo.ClientID = "node1"

			// read the node private and public keys for use in this interaction
			_, err = os.ReadFile(securityLocation + string(os.PathSeparator) + "node_key")
			g.Assert(err).IsNil()
			// clientPrivateKey, err := security.ParseRsaPrivateKeyFromPem(content)

			content, err := os.ReadFile(securityLocation + string(os.PathSeparator) + "node_key.pub")
			g.Assert(err).IsNil()
			publicKey, err := security.ParseRsaPublicKeyFromPem(content)
			g.Assert(err).IsNil()

			// clientPrivateKey, publicKey := security.GenerateRsaKeyPair()
			publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
			g.Assert(err).IsNil()
			clientInfo.PublicKey = []byte(publicKeyPem)

			clientJSON, err := json.Marshal(clientInfo)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(clientJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register acl for client
			acls := make([]*security.AccessControl, 0)
			acl1 := &security.AccessControl{}
			acl1.Action = "write"
			acl1.Resource = "/datasets*"
			acls = append(acls, acl1)
			aclJSON, err := json.Marshal(acls)
			g.Assert(err).IsNil()

			reqURL = datahubURL + "security/clients/node1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(aclJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

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
			g.Assert(err).IsNil()
			reqURL = datahubURL + "provider/logins"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqURL, bytes.NewBuffer(providerJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

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
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(201)

			// run job
			req, _ = http.NewRequest("PUT", datahubURL+"job/sync-from-remote-dataset-with-node-provider/run", nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			for {
				time.Sleep(100 * time.Millisecond)
				// check job status
				req, _ = http.NewRequest("GET", datahubURL+"jobs/_/history", nil)
				req.Header = http.Header{
					"Content-Type":  []string{"application/json"},
					"Authorization": []string{"Bearer " + token},
				}
				res, err = http.DefaultClient.Do(req)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotNil()
				g.Assert(res.StatusCode).Eql(200)
				body, _ := io.ReadAll(res.Body)
				var hist []map[string]interface{}
				_ = json.Unmarshal(body, &hist)
				// t.Log(hist)
				if len(hist) != 0 {
					g.Assert(hist[0]["lastError"]).IsZero("no error expected")
					break
				}

			}
		})

		/*
			g.It("Should remove client access", func() {
				// create new dataset
				res, err := http.Post(datahubURL, "application/json", strings.NewReader(""))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
			})

			g.It("Should retain users and acls after restart", func() {
				// create new dataset
				res, err := http.Post(datahubURL, "application/json", strings.NewReader(""))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
			})

			g.It("allow roles to be allocated to a client", func() {
				// create new dataset
				res, err := http.Post(datahubURL, "application/json", strings.NewReader(""))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
			})
		*/
	})
}
