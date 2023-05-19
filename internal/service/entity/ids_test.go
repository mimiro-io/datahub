// Copyright 2023 MIMIRO AS
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

package entity

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mimiro-io/datahub/internal/service/namespace"
	"github.com/mimiro-io/datahub/internal/service/types"
)

type nsMock struct{}

func (n nsMock) LookupNamespaceExpansion(prefix types.Prefix) (types.URI, error) {
	if prefix == "foo" {
		return "http://foo", nil
	}
	return "", errors.New("unknown prefix")
}

func (n nsMock) LookupExpansionPrefix(input types.URI) (types.Prefix, error) {
	if input == "http://foo/" {
		return "foo", nil
	}

	return "", errors.New("unknown namespace")
}

func TestEntity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "service/Entity Suite")
}

var _ = Describe("asCURIE", func() {
	mockNamespaces := namespace.NewManager(nsMock{})
	It("should return curie for valid curie string", func() {
		res, err := Lookup{namespaces: mockNamespaces}.asCURIE("foo:bar")
		Expect(err).To(BeNil())
		Expect(res).To(Equal(types.CURIE("foo:bar")))
	})
	It("should fail valid curie string with unknown prefix", func() {
		_, err := Lookup{namespaces: mockNamespaces}.asCURIE("hello:world")
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("unknown prefix"))
	})
	It("should return for curie with prefix only", func() {
		res, err := Lookup{namespaces: mockNamespaces}.asCURIE("foo:")
		Expect(err).To(BeNil())
		Expect(res).To(Equal(types.CURIE("foo:")))
	})
	It("should fail simple string", func() {
		_, err := Lookup{namespaces: mockNamespaces}.asCURIE("foo")
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("input foo is neither in CURIE format (prefix:value) nor a URI"))
	})
	It("should return curie for uri with known namespace", func() {
		res, err := Lookup{namespaces: mockNamespaces}.asCURIE("http://foo/bar")
		Expect(err).To(BeNil())
		Expect(res).To(Equal(types.CURIE("foo:bar")))
	})
	It("should fail valid uri string with unknown namespace", func() {
		_, err := Lookup{namespaces: mockNamespaces}.asCURIE("http://hello/world")
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("unknown namespace"))
	})
})
