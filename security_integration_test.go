package datahub

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/mimiro-io/datahub/internal/security"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/franela/goblin"
	"go.uber.org/fx"
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
			app, _ = Start(context.Background())
			os.Stdout = oldOut
			os.Stderr = oldErr
		})
		g.After(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			err := app.Stop(ctx)
			defer cancel()
			g.Assert(err).IsNil()
			err = os.RemoveAll(location)
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

			reqUrl := datahubURL + "security/token"
			res, err := http.PostForm(reqUrl, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqUrl = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqUrl, nil)
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

			reqUrl := datahubURL + "security/token"
			res, err := http.PostForm(reqUrl, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqUrl = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqUrl, nil)
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
			clientInfo.ClientId = "client1"
			_, publicKey := security.GenerateRsaKeyPair()
			publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
			clientInfo.PublicKey = []byte(publicKeyPem)

			clientJSON, err := json.Marshal(clientInfo)

			reqUrl = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(clientJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// get list of registered clients
			reqUrl = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqUrl, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)
			clientData, err := io.ReadAll(res.Body)
			var clients map[string]*security.ClientInfo
			err = json.Unmarshal(clientData, &clients)
			g.Assert(err).IsNil()

			g.Assert(len(clients)).Equal(1)
			g.Assert(clients["client1"].PublicKey).Equal([]byte(publicKeyPem))

			// delete client
			clientInfo = &security.ClientInfo{}
			clientInfo.ClientId = "client1"
			clientInfo.Deleted = true
			clientJSON, err = json.Marshal(clientInfo)

			reqUrl = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(clientJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// get clients and check client1 is not there
			reqUrl = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqUrl, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = client.Do(req)
			clientData, err = io.ReadAll(res.Body)
			cd := string(clientData)
			cd = cd + ""
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

			reqUrl = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(aclJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// fetch client acls
			reqUrl = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqUrl, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = client.Do(req)
			clientData, err = io.ReadAll(res.Body)
			var clientacls []*security.AccessControl
			err = json.Unmarshal(clientData, &clientacls)
			g.Assert(err).IsNil()
			g.Assert(len(clientacls)).Equal(1)

			// clear client acls
			reqUrl = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("DELETE", reqUrl, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// register client roles
			reqUrl = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqUrl, nil)
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
			res, err = client.Do(req)
			clientData, err = io.ReadAll(res.Body)
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

			reqUrl := datahubURL + "security/token"
			res, err := http.PostForm(reqUrl, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqUrl = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqUrl, nil)
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
			clientInfo.ClientId = "client1"
			clientPrivateKey, publicKey := security.GenerateRsaKeyPair()
			publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
			clientInfo.PublicKey = []byte(publicKeyPem)

			clientJSON, err := json.Marshal(clientInfo)

			reqUrl = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(clientJSON))
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

			reqUrl = datahubURL + "security/clients/client1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(aclJSON))
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
			data1.Set("client_assertion", pem)

			reqUrl = datahubURL + "security/token"
			res, err = http.PostForm(reqUrl, data1)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder = json.NewDecoder(res.Body)
			response = make(map[string]interface{})
			err = decoder.Decode(&response)
			clientToken := response["access_token"].(string)
			g.Assert(clientToken).IsNotNil()

			// use token to list datasets
			reqUrl = datahubURL + "datasets"
			client = &http.Client{}
			req, _ = http.NewRequest("GET", reqUrl, nil)
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
			g.Assert(len(result) == 1)
		})

		g.It("Should allow access to self via job and node jwt provider", func() {
			g.Timeout(15 * time.Minute)

			// get jwt for admin
			data := url.Values{}
			data.Set("grant_type", "client_credentials")
			data.Set("client_id", "admin")
			data.Set("client_secret", "admin")

			reqUrl := datahubURL + "security/token"
			res, err := http.PostForm(reqUrl, data)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			decoder := json.NewDecoder(res.Body)
			response := make(map[string]interface{})
			err = decoder.Decode(&response)
			token := response["access_token"].(string)
			g.Assert(token).IsNotNil()

			// check JWT valid for endpoint access (also tests the admin role)
			reqUrl = datahubURL + "datasets"
			client := &http.Client{}
			req, _ := http.NewRequest("GET", reqUrl, nil)
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
			clientInfo.ClientId = "node1"

			// read the node private and public keys for use in this interaction
			content, err := ioutil.ReadFile(securityLocation + string(os.PathSeparator) + "node_key")
			// clientPrivateKey, err := security.ParseRsaPrivateKeyFromPem(content)

			content, err = ioutil.ReadFile(securityLocation + string(os.PathSeparator) + "node_key.pub")
			publicKey, err := security.ParseRsaPublicKeyFromPem(content)

			// clientPrivateKey, publicKey := security.GenerateRsaKeyPair()
			publicKeyPem, err := security.ExportRsaPublicKeyAsPem(publicKey)
			clientInfo.PublicKey = []byte(publicKeyPem)

			clientJSON, err := json.Marshal(clientInfo)

			reqUrl = datahubURL + "security/clients"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(clientJSON))
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

			reqUrl = datahubURL + "security/clients/node1/acl"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(aclJSON))
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
			reqUrl = datahubURL + "provider/logins"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer(providerJSON))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(200)

			// upload job to access remote (loopback) dataset
			jobJson := `{
			"id" : "sync-from-remote-dataset-with-node-provider",
			"triggers": [{"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2s"}],
			"fullSyncSchedule" : "@every 2s",
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "` + datahubURL + `datasets/core.Dataset/entities",
				"TokenProvider" : "node1provider"
			},
			"sink" : {
				"Type" : "DevNullSink"
			}}`

			reqUrl = datahubURL + "jobs"
			client = &http.Client{}
			req, _ = http.NewRequest("POST", reqUrl, bytes.NewBuffer([]byte(jobJson)))
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}

			res, err = client.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotNil()
			g.Assert(res.StatusCode).Eql(201)

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
