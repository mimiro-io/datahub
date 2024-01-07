package datahub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mimiro-io/datahub/internal/conf"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/mimiro-io/datahub"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type (
	enviromentConfig struct {
		NODE_ID                  string
		AUTHORIZATION_MIDDLEWARE string
		ADMIN_USERNAME           string
		ADMIN_PASSWORD           string
		TOKEN_WELL_KNOWN         string
		TOKEN_ISSUER             string
		TOKEN_AUDIENCE           string
		OPA_ENDPOINT             string
	}

	userConfig struct {
		AuthorizationOpa bool
		AuthorizationACL bool
		bobInNodeSec     bool
	}
	requestDetails struct {
		path      string
		tokenFrom tokenSource
		user      string
	}

	testOutcome struct {
		status int
		body   string
	}
	tokenSource int
)

var (
	Datasets200              = testOutcome{status: 200, body: `[{"Name":"core.Dataset"},{"Name":"people"},{"Name":"places"}]`}
	Jobs200                  = testOutcome{status: 200, body: "[]"}
	MissingJwt401            = testOutcome{status: 401, body: `{"message":"code=400, message=missing or malformed jwt"}`}
	SignatureWrongNodeSec401 = testOutcome{status: 401, body: `{"message":"NodeSec: crypto/rsa: verification error"}`}
	SignatureWrongOauth401   = testOutcome{status: 401, body: `{"message":"Oauth: crypto/rsa: verification error"}`}
	NoAccess403              = testOutcome{status: 403, body: `{"message":"user does not have permission"}`}
	ServerPanic              = testOutcome{status: -1, body: "panic"}
)

const (
	location         string = "./access_matrix_integration_test_store"
	oauthLocation    string = "./access_matrix_integration_test_oauth_store"
	securityLocation string = "./access_matrix_integration_test_store"

	adminPwd       tokenSource = iota
	nodeSecValid   tokenSource = iota
	nodeSecInvalid tokenSource = iota
	oauthValid     tokenSource = iota
	oauthInvalid   tokenSource = iota
)

var (
	localCases = securedCases("local")
	opaCases   = securedCases("opa")
	onCases    = securedCases("on")
	added      = false
)

func securedCases(authorizer string) []TableEntry {
	return []TableEntry{
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", not start without admin user and admin password",
			requestDetails{path: "/datasets"},
			userConfig{},
			enviromentConfig{AUTHORIZATION_MIDDLEWARE: authorizer},
			ServerPanic,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", not start without admin user",
			requestDetails{path: "/datasets"},
			userConfig{},
			enviromentConfig{AUTHORIZATION_MIDDLEWARE: authorizer, ADMIN_PASSWORD: "bar"},
			ServerPanic,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", not start without admin password",
			requestDetails{path: "/datasets"},
			userConfig{},
			enviromentConfig{AUTHORIZATION_MIDDLEWARE: authorizer, ADMIN_USERNAME: "foo"},
			ServerPanic,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", no token",
			requestDetails{path: "/datasets"},
			userConfig{},
			enviromentConfig{AUTHORIZATION_MIDDLEWARE: authorizer, ADMIN_USERNAME: "foo", ADMIN_PASSWORD: "bar"},
			MissingJwt401,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", missing wellknown config",
			requestDetails{path: "/datasets", tokenFrom: oauthValid, user: "bob"},
			userConfig{},
			enviromentConfig{AUTHORIZATION_MIDDLEWARE: authorizer, ADMIN_USERNAME: "foo", ADMIN_PASSWORD: "bar"},
			SignatureWrongNodeSec401,
		), // response from nodesec. the token would be valid in oauth but datahub does not check there since config missing
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", invalid signature",
			requestDetails{path: "/datasets", tokenFrom: oauthInvalid, user: "bob"},
			userConfig{},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			SignatureWrongOauth401,
		), // response from oauth
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", admin access",
			requestDetails{path: "/datasets", tokenFrom: adminPwd},
			userConfig{},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			Datasets200,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", admin access 2",
			requestDetails{path: "/jobs", tokenFrom: adminPwd},
			userConfig{},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			Jobs200,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, no permissions",
			requestDetails{path: "/datasets", tokenFrom: oauthValid, user: "bob"},
			userConfig{},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			NoAccess403,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, unallowed path",
			requestDetails{path: "/jobs", tokenFrom: oauthValid, user: "bob"}, // unauthorized path
			userConfig{
				AuthorizationACL: true,
			}, // give acl for datasets to bob, not for jobs
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			NoAccess403,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, unallowed path 2",
			requestDetails{path: "/jobs", tokenFrom: oauthValid, user: "bob"}, // unauthorized path
			userConfig{AuthorizationOpa: false},                               // let OPA disallow requested path
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			NoAccess403,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, valid path acl",
			requestDetails{path: "/datasets", tokenFrom: oauthValid, user: "bob"},
			userConfig{
				AuthorizationACL: true,
			}, // give acl for datasets listing and dataset:places to bob
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			testOutcome{status: 200, body: `[{"Name":"places"}]`},
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, valid path opa",
			requestDetails{path: "/jobs", tokenFrom: oauthValid, user: "bob"},
			userConfig{AuthorizationOpa: true}, // let OPA allow jobs
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			Jobs200,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, valid path opa 2",
			requestDetails{path: "/datasets", tokenFrom: oauthValid, user: "bob"},
			userConfig{AuthorizationOpa: true}, // let OPA allow datasets listing, and adding people access
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			testOutcome{status: 200, body: `[{"Name":"people"}]`},
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid oauth, valid path opa and acl",
			requestDetails{path: "/datasets", tokenFrom: oauthValid, user: "bob"},
			userConfig{AuthorizationOpa: true, AuthorizationACL: true},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			testOutcome{status: 200, body: `[{"Name":"people"},{"Name":"places"}]`},
		),

		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, no permissions",
			requestDetails{path: "/datasets", tokenFrom: nodeSecValid, user: "bob"},
			userConfig{},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			NoAccess403,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, unallowed path",
			requestDetails{path: "/jobs", tokenFrom: nodeSecValid, user: "bob"}, // unauthorized path
			userConfig{
				AuthorizationACL: true,
			}, // give acl for datasets to bob, not for jobs
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			NoAccess403,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, unallowed path 2",
			requestDetails{path: "/jobs", tokenFrom: nodeSecValid, user: "bob"}, // unauthorized path
			userConfig{AuthorizationOpa: false},                                 // let OPA disallow requested path
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			NoAccess403,
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, valid path acl",
			requestDetails{path: "/datasets", tokenFrom: nodeSecValid, user: "bob"}, // unauthorized path
			userConfig{
				AuthorizationACL: true,
			}, // give acl for datasets to bob, not for jobs
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
			},
			testOutcome{status: 200, body: `[{"Name":"places"}]`},
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, valid path opa",
			requestDetails{path: "/jobs", tokenFrom: nodeSecValid, user: "bob"},
			userConfig{AuthorizationOpa: true}, // let OPA allow jobs
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			Jobs200,
		), Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, valid path opa 2",
			requestDetails{path: "/datasets", tokenFrom: nodeSecValid, user: "bob"},
			userConfig{AuthorizationOpa: true}, // let OPA allow datasets listing, and adding people access
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			testOutcome{status: 200, body: `[{"Name":"people"}]`},
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, valid path opa and acl",
			requestDetails{path: "/datasets", tokenFrom: nodeSecValid, user: "bob"},
			userConfig{AuthorizationOpa: true, AuthorizationACL: true},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			testOutcome{status: 200, body: `[{"Name":"people"},{"Name":"places"}]`},
		),
		Entry(
			"With AUTHORIZATION_MIDDLEWARE="+authorizer+", valid nodeSec, invalid path opa and acl",
			requestDetails{path: "/datasets/places/changes", tokenFrom: nodeSecValid, user: "bob"},
			userConfig{AuthorizationOpa: false, AuthorizationACL: true},
			enviromentConfig{
				AUTHORIZATION_MIDDLEWARE: authorizer,
				ADMIN_USERNAME:           "foo",
				ADMIN_PASSWORD:           "bar",
				TOKEN_ISSUER:             "http://localhost:14447",
				TOKEN_AUDIENCE:           "http://localhost:24978",
				TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				OPA_ENDPOINT:             "http://localhost:14446/",
			},
			NoAccess403,
		),
	}
}

var (
	opaState = new(bool)
	_        = Describe("Access", Ordered, func() {
		var wellKnownServer *http.Server
		BeforeAll(func() {
			// start an external token validation endpoint
			os.MkdirAll(oauthLocation, 0o755)
			os.WriteFile(oauthLocation+"/well-known.json", []byte(wellKnown), 0o644)

			mux := http.NewServeMux()
			mux.Handle("/", http.FileServer(http.Dir(oauthLocation)))
			mux.HandleFunc("/v1/data/datahub/authz/allow", func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"result":%v}`, *opaState)
			})
			mux.HandleFunc("/v1/data/datahub/authz/datasets", func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"result":["people"]}`)
			})
			wellKnownServer = &http.Server{Addr: ":14446", Handler: mux}
			go func() {
				err := wellKnownServer.ListenAndServe()
				if err != nil && err.Error() != "http: Server closed" {
					panic(err)
				}
			}()
		})
		AfterAll(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			err := wellKnownServer.Shutdown(ctx)
			os.RemoveAll(oauthLocation)
			os.Remove(location)
			os.RemoveAll(securityLocation)
			Expect(err).To(BeNil())
			defer cancel()
		})

		DescribeTable(
			"Decision Matrix",
			execEntry,
			describeEntry,

			Entry(
				"With no config, be unsecured",
				requestDetails{path: "/datasets"},
				userConfig{},
				enviromentConfig{},
				Datasets200,
			),
			Entry(
				"With no config, be unsecured 2",
				requestDetails{path: "/jobs"},
				userConfig{},
				enviromentConfig{},
				Jobs200,
			),

			Entry(
				"With AUTHORIZATION_MIDDLEWARE=noop, be unsecured",
				requestDetails{path: "/datasets"},
				userConfig{},
				enviromentConfig{AUTHORIZATION_MIDDLEWARE: "noop"},
				Datasets200,
			),
			Entry(
				"With AUTHORIZATION_MIDDLEWARE=noop, be unsecured 2",
				requestDetails{path: "/jobs"},
				userConfig{},
				enviromentConfig{AUTHORIZATION_MIDDLEWARE: "noop"},
				Jobs200,
			),
			Entry(
				"With AUTHORIZATION_MIDDLEWARE=on, admin access, underlying error",
				requestDetails{path: "/datasets/unknown", tokenFrom: adminPwd},
				userConfig{},
				enviromentConfig{
					AUTHORIZATION_MIDDLEWARE: "on",
					ADMIN_USERNAME:           "foo",
					ADMIN_PASSWORD:           "bar",
					TOKEN_ISSUER:             "http://localhost:14447",
					TOKEN_AUDIENCE:           "http://localhost:24978",
					TOKEN_WELL_KNOWN:         "http://localhost:14446/well-known.json",
				},
				testOutcome{
					status: 404,
				},
			),

			// both on, opa and local values "turn on" the authorization middleware and implicitly authentication
			//localCases,
			//opaCases,
			onCases,
		)
	})
)

func describeEntry(r requestDetails, u userConfig, ec enviromentConfig, expectedOutcome testOutcome) string {
	return fmt.Sprintf(
		":%v Expected. When the request is %+v for userConfig %+v, while the server config is %+v",
		expectedOutcome,
		r,
		u,
		ec,
	)
}

func execEntry(r requestDetails, u userConfig, ec enviromentConfig, expectedOutcome testOutcome) {
	//_ = os.RemoveAll(location)
	//_ = os.RemoveAll(securityLocation)
	_ = os.Setenv("STORE_LOCATION", location)
	_ = os.Setenv("PROFILE", "test")
	_ = os.Setenv("SERVER_PORT", "24978")
	_ = os.Setenv("GC_ON_STARTUP", "false")

	_ = os.Setenv("AUTHORIZATION_MIDDLEWARE", ec.AUTHORIZATION_MIDDLEWARE)
	_ = os.Setenv("ADMIN_USERNAME", ec.ADMIN_USERNAME)
	_ = os.Setenv("ADMIN_PASSWORD", ec.ADMIN_PASSWORD)
	_ = os.Setenv("NODE_ID", ec.NODE_ID)
	_ = os.Setenv("TOKEN_WELL_KNOWN", ec.TOKEN_WELL_KNOWN)
	_ = os.Setenv("TOKEN_AUDIENCE", ec.TOKEN_AUDIENCE)
	_ = os.Setenv("TOKEN_ISSUER", ec.TOKEN_ISSUER)
	_ = os.Setenv("SECURITY_STORAGE_LOCATION", securityLocation)
	_ = os.Setenv("OPA_ENDPOINT", ec.OPA_ENDPOINT)

	defer func() {
		//_ = os.RemoveAll(location)
		//_ = os.RemoveAll(securityLocation)
		_ = os.Unsetenv("PROFILE")
		_ = os.Unsetenv("SERVER_PORT")
		_ = os.Unsetenv("STORE_LOCATION")
		_ = os.Unsetenv("GC_ON_STARTUP")

		_ = os.Unsetenv("AUTHORIZATION_MIDDLEWARE")
		_ = os.Unsetenv("ADMIN_USERNAME")
		_ = os.Unsetenv("ADMIN_PASSWORD")
		_ = os.Unsetenv("NODE_ID")
		_ = os.Unsetenv("TOKEN_WELL_KNOWN")
		_ = os.Unsetenv("TOKEN_AUDIENCE")
		_ = os.Unsetenv("TOKEN_ISSUER")
		_ = os.Unsetenv("SECURITY_STORAGE_LOCATION")
		_ = os.Unsetenv("OPA_ENDPOINT")
	}()
	status := 200
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	app := startDh(&status)
	defer func() {
		if app != nil {
			app.Stop(ctx)
		}
	}()

	if status > -1 {
		addDatasets("http://localhost:24978/", ec.ADMIN_USERNAME, ec.ADMIN_PASSWORD)
		if u.AuthorizationACL {
			giveBobACLForPaths("http://localhost:24978/", ec.ADMIN_USERNAME, ec.ADMIN_PASSWORD, "/datasets", "/datasets/places")
		} else {
			giveBobACLForPaths("http://localhost:24978/", ec.ADMIN_USERNAME, ec.ADMIN_PASSWORD)
		}
		*opaState = u.AuthorizationOpa
		reqURL := "http://localhost:24978" + r.path
		req, _ := http.NewRequest("GET", reqURL, nil)
		if r.tokenFrom > 0 {
			token := getToken(r.tokenFrom, ec, r)
			if token == "invalid" {
				panic("invalid token source")
			}
			req.Header = http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer " + token},
			}
		} else {
			req.Header = http.Header{
				"Content-Type": []string{"application/json"},
			}
		}
		response, err := http.DefaultClient.Do(req)
		Expect(err).To(BeNil())
		// fmt.Printf("%+v, %+v, %+v", app.Err(), response, err)
		if response != nil {
			status = response.StatusCode
			resTxt, err := io.ReadAll(response.Body)
			Expect(err).To(BeNil())

			Expect(status).To(BeEquivalentTo(expectedOutcome.status))

			jsonReceived := map[string]any{}
			if len(resTxt) > 0 {
				err = json.Unmarshal([]byte(fmt.Sprintf(`{"r":%s}`, resTxt)), &jsonReceived)
				Expect(err).To(BeNil())
			}
			jsonExpected := map[string]any{}
			if len(expectedOutcome.body) > 0 {
				err = json.Unmarshal([]byte(fmt.Sprintf(`{"r":%s}`, expectedOutcome.body)), &jsonExpected)
				Expect(err).To(BeNil())
			}
			Expect(jsonReceived).To(BeEquivalentTo(jsonExpected))
		}
		response.Body.Close()
	}
	if app != nil {
		app.Stop(ctx)
	}
	cancel()
	// Expect(status).To(BeEquivalentTo(expectedOutcome.status))
}

func addDatasets(datahubURL string, username string, password string) {
	GinkgoHelper()
	if added {
		return
	}
	adminToken := getAdminToken(datahubURL, username, password)
	reqURL := datahubURL + "datasets/people"
	req, _ := http.NewRequest("POST", reqURL, nil)
	req.Header = http.Header{
		"Content-Type":  []string{"application/json"},
		"Authorization": []string{"Bearer " + adminToken},
	}

	res, err := http.DefaultClient.Do(req)
	Expect(err).To(BeNil())
	Expect(res).NotTo(BeNil())
	Expect(res.StatusCode).To(Equal(200))

	reqURL = datahubURL + "datasets/places"
	req, _ = http.NewRequest("POST", reqURL, nil)
	req.Header = http.Header{
		"Content-Type":  []string{"application/json"},
		"Authorization": []string{"Bearer " + adminToken},
	}

	res, err = http.DefaultClient.Do(req)
	Expect(err).To(BeNil())
	Expect(res).NotTo(BeNil())
	Expect(res.StatusCode).To(Equal(200))
	added = true
}

func getToken(from tokenSource, ec enviromentConfig, details requestDetails) string {
	GinkgoHelper()
	switch from {
	case adminPwd:
		return getAdminToken("http://localhost:24978/", "foo", "bar")
	case oauthInvalid:
		t, err := createOauthJwtToken(invalidPrivateKey, details.user, ec.TOKEN_ISSUER, ec.TOKEN_AUDIENCE)
		Expect(err).To(BeNil())
		return t
	case oauthValid:
		t, err := createOauthJwtToken(privateKey, details.user, ec.TOKEN_ISSUER, ec.TOKEN_AUDIENCE)
		Expect(err).To(BeNil())
		return t
	case nodeSecValid:
		adminToken := getAdminToken("http://localhost:24978/", "foo", "bar")
		clientPrivateKey := registerNodeSecClient("http://localhost:24978/", adminToken, details.user)
		t := createNodeSecToken("http://localhost:24978/", details.user, ec.NODE_ID, clientPrivateKey)
		return t
	default:
		return "invalid"
	}
}

func startDh(status *int) *datahub.DatahubInstance {
	oldOut := os.Stdout
	oldErr := os.Stderr
	devNull, _ := os.Open("/dev/null")
	os.Stdout = devNull
	os.Stderr = devNull
	defer func() {
		if err := recover(); err != nil {
			// log.Println("panic occurred:", err)
			*status = -1
		}
	}()
	config, _ := conf.LoadConfig("")
	dhi, _ := datahub.NewDatahubInstance(config)
	go dhi.Start()
	os.Stdout = oldOut
	os.Stderr = oldErr
	return dhi
}
