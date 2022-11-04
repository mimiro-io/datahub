package entity

import (
	"errors"
	"testing"

	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/service/namespace"
	"github.com/mimiro-io/datahub/internal/service/types"
)

type nsMock struct{}

func (n nsMock) LookupNamespaceExpansion(prefix types.Prefix) (types.URI, error) {
	if "foo" == prefix {
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

func TestLookup_asCURIE(t *testing.T) {
	mockNamespaces := namespace.NewManager(nsMock{})
	g := goblin.Goblin(t)
	g.Describe("asCURIE", func() {
		g.It("should return curie for valid curie string", func() {
			res, err := Lookup{namespaces: mockNamespaces}.asCURIE("foo:bar")
			g.Assert(err).IsNil()
			g.Assert(res).Equal(types.CURIE("foo:bar"))
		})
		g.It("should fail valid curie string with unknown prefix", func() {
			_, err := Lookup{namespaces: mockNamespaces}.asCURIE("hello:world")
			g.Assert(err).IsNotNil()
			g.Assert(err.Error()).Equal("unknown prefix")
		})
		g.It("should return for curie with prefix only", func() {
			res, err := Lookup{namespaces: mockNamespaces}.asCURIE("foo:")
			g.Assert(err).IsNil()
			g.Assert(res).Equal(types.CURIE("foo:"))
		})
		g.It("should fail simple string", func() {
			_, err := Lookup{namespaces: mockNamespaces}.asCURIE("foo")
			g.Assert(err).IsNotNil()
			g.Assert(err.Error()).Equal("input foo is neither in CURIE format (prefix:value) nor a URI")
		})
		g.It("should return curie for uri with known namespace", func() {
			res, err := Lookup{namespaces: mockNamespaces}.asCURIE("http://foo/bar")
			g.Assert(err).IsNil()
			g.Assert(res).Equal(types.CURIE("foo:bar"))
		})
		g.It("should fail valid uri string with unknown namespace", func() {
			_, err := Lookup{namespaces: mockNamespaces}.asCURIE("http://hello/world")
			g.Assert(err).IsNotNil()
			g.Assert(err.Error()).Equal("unknown namespace")
		})
	})
}

func TestLookup_internalIdForCURIE(t *testing.T) {
}
