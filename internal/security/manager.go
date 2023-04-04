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

package security

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

// NodeInfo is a data structure that represents a node in the security topology
type NodeInfo struct {
	NodeId   string
	KeyPairs []*KeyPair
}

func NewNodeInfo(nodeId string, keyPairs []*KeyPair) *NodeInfo {
	nodeInfo := &NodeInfo{}
	nodeInfo.NodeId = nodeId
	nodeInfo.KeyPairs = keyPairs
	return nodeInfo
}

type KeyPair struct {
	PrivateKey *rsa.PrivateKey
	PublicKey  *rsa.PublicKey
	Active     bool
	Expires    uint64
}

func NewKeyPair(privateKey *rsa.PrivateKey, publicKey *rsa.PublicKey, active bool) *KeyPair {
	keyPair := &KeyPair{}
	keyPair.PrivateKey = privateKey
	keyPair.PublicKey = publicKey
	keyPair.Active = active
	return keyPair
}

type ClientInfo struct {
	ClientId  string
	PublicKey []byte
	Deleted   bool
}

// ClientIdClaim used by a client to assert it is who they say they are.
type ClientIdClaim struct {
	clientId       string
	timestamp      string
	Message        []byte // encrypted message
	MessageHashSum int    // signed
	Signature      []byte
	Algorithm      string
}

// AccessControl allows or denies action on a resource
// Allowed actions are: read, write (write implies delete)
// Resources are: nodeid/datasets/[name] with *
// nodeid/* gives access to all endpoints
// nodeid/jobs/*
// nodeid/content/name or *
type AccessControl struct {
	Resource string
	Action   string
	Deny     bool
}

// ServiceCore provides core logic for management of data and verification of client claims and requests for access
// of resources.
type ServiceCore struct {
	// admin client key for node admin
	AdminClientKey string

	// admin client secret for node admin
	AdminClientSecret string

	// storage location for this node's data
	Location string

	// this node info
	NodeInfo *NodeInfo

	// client id keyed client info
	clients sync.Map

	// client id keyed list of access controls
	accessControls sync.Map

	// client id keyed list of roles
	roles sync.Map

	// indicates if local authorisation is enabled
	IsLocalAuthEnabled bool
}

func NewServiceCore(env *conf.Env) *ServiceCore {
	serviceCore := &ServiceCore{}
	serviceCore.Location = env.SecurityStorageLocation
	serviceCore.AdminClientKey = env.AdminUserName
	serviceCore.AdminClientSecret = env.AdminPassword
	nodeInfo := NewNodeInfo(env.NodeId, nil)
	serviceCore.NodeInfo = nodeInfo
	serviceCore.IsLocalAuthEnabled = env.Auth.Middleware == "local"

	serviceCore.Init()

	jwt.MarshalSingleStringAsArray = false

	return serviceCore
}

func GenerateRsaKeyPair() (*rsa.PrivateKey, *rsa.PublicKey) {
	key, _ := rsa.GenerateKey(rand.Reader, 4096)
	return key, &key.PublicKey
}

func ExportRsaPrivateKeyAsPem(key *rsa.PrivateKey) (string, error) {
	bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", err
	}
	pemBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: bytes,
		},
	)
	return string(pemBytes), nil
}

func ParseRsaPrivateKeyFromPem(pemValue []byte) (*rsa.PrivateKey, error) {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(pemValue)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func ExportRsaPublicKeyAsPem(key *rsa.PublicKey) (string, error) {
	bytes, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return "", err
	}

	pemBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: bytes,
		},
	)

	return string(pemBytes), nil
}

func ParseRsaPublicKeyFromPem(pemValue []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemValue)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	switch pub := pub.(type) {
	case *rsa.PublicKey:
		return pub, nil
	default:
		break // fall through
	}
	return nil, errors.New("Key type is not RSA")
}

// Init ensures that local storage is available
func (serviceCore *ServiceCore) Init() error {
	// ensure location exists
	err := os.MkdirAll(serviceCore.Location, os.ModePerm)
	if err != nil {
		return err
	}

	// try load private and public key
	fileinfo, err := os.Stat(serviceCore.Location + string(os.PathSeparator) + "node_key")
	if err == nil {
		// load data for private key
		content, err := ioutil.ReadFile(serviceCore.Location + string(os.PathSeparator) + fileinfo.Name())
		if err != nil {
			return err
		}

		privateKey, err := ParseRsaPrivateKeyFromPem(content)
		if err != nil {
			return err
		}

		// public key
		content, err = ioutil.ReadFile(serviceCore.Location + string(os.PathSeparator) + "node_key.pub")
		if err != nil {
			return err
		}

		publicKey, err := ParseRsaPublicKeyFromPem(content)
		if err != nil {
			return err
		}

		keyPair := NewKeyPair(privateKey, publicKey, true)
		serviceCore.NodeInfo.KeyPairs = make([]*KeyPair, 0)
		serviceCore.NodeInfo.KeyPairs = append(serviceCore.NodeInfo.KeyPairs, keyPair)
	} else {
		// generate files
		privateKey, publicKey := GenerateRsaKeyPair()
		privateKeyPem, err := ExportRsaPrivateKeyAsPem(privateKey)
		if err != nil {
			return err
		}
		publicKeyPem, err := ExportRsaPublicKeyAsPem(publicKey)
		if err != nil {
			return err
		}

		// write keys to files
		err = ioutil.WriteFile(serviceCore.Location+string(os.PathSeparator)+"node_key", []byte(privateKeyPem), 0600)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(serviceCore.Location+string(os.PathSeparator)+"node_key.pub", []byte(publicKeyPem), 0600)
		if err != nil {
			return err
		}

		keyPair := NewKeyPair(privateKey, publicKey, true)
		serviceCore.NodeInfo.KeyPairs = make([]*KeyPair, 0)
		serviceCore.NodeInfo.KeyPairs = append(serviceCore.NodeInfo.KeyPairs, keyPair)
	}

	// load clients
	err = serviceCore.loadClients()
	if err != nil {
		return err
	}

	// load acls
	err = serviceCore.loadAcls()
	if err != nil {
		return err
	}

	return nil
}

func (serviceCore *ServiceCore) loadClients() error {
	clients := make(map[string]*ClientInfo)
	jsondata, err := ioutil.ReadFile(serviceCore.Location + string(os.PathSeparator) + "clients.json")
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsondata, &clients)
	if err != nil {
		return err
	}

	for clientId, clientInfo := range clients {
		serviceCore.clients.Store(clientId, clientInfo)
	}

	return nil
}

func (serviceCore *ServiceCore) loadAcls() error {
	acls := make(map[string][]*AccessControl)
	jsondata, err := ioutil.ReadFile(serviceCore.Location + string(os.PathSeparator) + "acls.json")
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsondata, &acls)
	if err != nil {
		return err
	}

	for clientId, acls := range acls {
		serviceCore.accessControls.Store(clientId, acls)
	}

	return nil
}

func CreateJWTForTokenRequest(subject string, audience string, privateKey *rsa.PrivateKey) (string, error) {
	uniqueId := uuid.New()

	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute * 1)),
		ID:        uniqueId.String(),
		Subject:   subject,
		Audience:  jwt.ClaimStrings{audience},
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(privateKey)
	if err != nil {
		return "", err
	}
	return token, nil
}

// CreateJWTForTokenRequest returns a JWT token that can be used to get an access token to a remote endpoint
func (serviceCore *ServiceCore) CreateJWTForTokenRequest(audience string) (string, error) {
	keyPair := serviceCore.GetActiveKeyPair()
	return CreateJWTForTokenRequest(serviceCore.NodeInfo.NodeId, audience, keyPair.PrivateKey)
}

func (serviceCore *ServiceCore) RegisterClient(clientInfo *ClientInfo) {
	var mut sync.Mutex
	mut.Lock()
	defer mut.Unlock()

	if clientInfo.Deleted {
		serviceCore.clients.Delete(clientInfo.ClientId)
		serviceCore.DeleteClientAccessControls(clientInfo.ClientId)
	} else {
		serviceCore.clients.Store(clientInfo.ClientId, clientInfo)
	}

	jsonData, _ := json.Marshal(serviceCore.GetClients())
	_ = ioutil.WriteFile(serviceCore.Location+string(os.PathSeparator)+"clients.json", jsonData, 0644)
}

func (serviceCore *ServiceCore) GetClients() map[string]*ClientInfo {
	m := make(map[string]*ClientInfo)
	serviceCore.clients.Range(func(k interface{}, v interface{}) bool {
		m[k.(string)] = v.(*ClientInfo)
		return true
	})
	return m
}

func (serviceCore *ServiceCore) DeleteClientAccessControls(clientId string) {
	var mut sync.Mutex
	mut.Lock()
	defer mut.Unlock()

	serviceCore.accessControls.Delete(clientId)

	jsonData, _ := json.Marshal(serviceCore.GetClients())
	_ = ioutil.WriteFile(serviceCore.Location+string(os.PathSeparator)+"acls.json", jsonData, 0644)
}

func (serviceCore *ServiceCore) SetClientAccessControls(clientId string, acls []*AccessControl) {
	var mut sync.Mutex
	mut.Lock()
	defer mut.Unlock()

	serviceCore.accessControls.Store(clientId, acls)

	jsonData, _ := json.Marshal(serviceCore.GetAllAccessControls())
	_ = ioutil.WriteFile(serviceCore.Location+string(os.PathSeparator)+"acls.json", jsonData, 0644)
}

func (serviceCore *ServiceCore) GetAccessControls(clientId string) []*AccessControl {
	acls, ok := serviceCore.accessControls.Load(clientId)
	if ok {
		return acls.([]*AccessControl)
	} else {
		return nil
	}
}

func (serviceCore *ServiceCore) GetAllAccessControls() map[string][]*AccessControl {
	m := make(map[string][]*AccessControl)
	serviceCore.accessControls.Range(func(k interface{}, v interface{}) bool {
		m[k.(string)] = v.([]*AccessControl)
		return true
	})
	return m
}

func (serviceCore *ServiceCore) GetActiveKeyPair() *KeyPair {
	return serviceCore.NodeInfo.KeyPairs[0]
}

func (serviceCore *ServiceCore) MakeAdminJWT(clientKey string, clientSecret string) (string, error) {

	if clientKey != serviceCore.AdminClientKey || clientSecret != serviceCore.AdminClientSecret {
		return "", errors.New("incorrect key or secret")
	}

	keypair := serviceCore.GetActiveKeyPair()
	roles := make([]string, 0)
	roles = append(roles, "admin")

	claims := CustomClaims{}
	claims.Roles = roles
	claims.RegisteredClaims =
		jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute * 15)),
			Issuer:    "node:" + serviceCore.NodeInfo.NodeId,
			Audience:  jwt.ClaimStrings{"node:" + serviceCore.NodeInfo.NodeId},
			Subject:   clientKey,
		}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(keypair.PrivateKey)
	if err != nil {
		return "", err
	}

	return token, nil
}

func (serviceCore *ServiceCore) ValidateClientJWTMakeJWTAccessToken(clientJWT string) (string, error) {
	// parse without key to get subject
	token, err := jwt.ParseWithClaims(clientJWT, &jwt.RegisteredClaims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(""), nil
	})

	clientClaims := token.Claims.(*jwt.RegisteredClaims)
	var clientId = clientClaims.Subject

	client, _ := serviceCore.clients.Load(clientId)
	clientPublicKey, err := ParseRsaPublicKeyFromPem(client.(*ClientInfo).PublicKey)

	// parse again with key
	token, err = jwt.ParseWithClaims(clientJWT, &jwt.RegisteredClaims{}, func(token *jwt.Token) (interface{}, error) {
		return clientPublicKey, nil
	})

	if token.Valid == false {
		return "", errors.New("invalid client jwt")
	}

	if err != nil {
		return "", errors.New("invalid client jwt")
	}

	// make a JWT
	roles := make([]string, 0)
	roles = append(roles, "client")

	// add in roles in config
	claims := CustomClaims{}
	claims.Roles = roles
	claims.RegisteredClaims =
		jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute * 15)),
			Issuer:    "node:" + serviceCore.NodeInfo.NodeId,
			Audience:  jwt.ClaimStrings{"node:" + serviceCore.NodeInfo.NodeId},
			Subject:   clientId,
		}

	keypair := serviceCore.GetActiveKeyPair()
	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(keypair.PrivateKey)
	if err != nil {
		return "", err
	}

	return accessToken, nil
}

// FilterDatasets given a list of datasets returns the ones that the user has access to
func (serviceCore *ServiceCore) FilterDatasets(datasets []server.DatasetName, subject string) ([]server.DatasetName, error) {
	acl := serviceCore.GetAccessControls(subject)
	result := make([]server.DatasetName, 0)

	for _, dataset := range datasets {
		for _, ac := range acl {
			if serviceCore.CheckGranted(ac, "/datasets/"+dataset.Name, "read") {
				result = append(result, dataset)
			}
		}
	}

	return result, nil
}

func (serviceCore *ServiceCore) CheckGranted(ac *AccessControl, resource string, action string) bool {
	if ac.Resource == resource {
		if action == "read" && (ac.Action == "read" || ac.Action == "write") {
			return !ac.Deny
		} else if action == ac.Action {
			return !ac.Deny
		}
	}

	// if the ac has a resource with trailing * this should be treated as a pattern
	// grants access to any resource that starts with this pattern and correct action
	if strings.HasSuffix(ac.Resource, "*") {
		pattern := ac.Resource[:len(ac.Resource)-1]
		if strings.HasPrefix(resource, pattern) {
			if action == "read" && (ac.Action == "read" || ac.Action == "write") {
				return !ac.Deny
			} else if action == ac.Action {
				return !ac.Deny
			}
		}
	}

	return false
}
