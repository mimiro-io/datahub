package datahub

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/mimiro-io/datahub/internal/security"
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
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := app.Stop(ctx)
			cancel()
			g.Assert(err).IsNil()
			err = os.RemoveAll(location)
			// err = os.RemoveAll(securityLocation)
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

		g.It("Should register client and allow access", func() {
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
		}) */
	})
}
