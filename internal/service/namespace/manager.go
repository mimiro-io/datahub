package namespace

import (
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"strings"
)

type (
	Manager interface {
		// ExtractPrefix
		//	Assumes that the input string is a CURIE string, and attempts to parse prefix from it
		//  the second return value is a flag indicating whether a prefix was found in the input or not
		ExtractPrefix(input string) (types.Prefix, bool)
		// ExpandPrefix lookup curie prefix in global namespace mapping
		ExpandPrefix(input types.Prefix) (types.URI, error)
		// ExtractNamespaceURI
		//   returns extracted namespace uri and local value (last path element of input uri)
		//   the 3rd return value indicates if namespace extraction was successful
		ExtractNamespaceURI(input string) (types.URI, string, bool)
		GetNamespacePrefix(input types.URI) (types.Prefix, error)
	}

	BadgerManager struct {
		store store.LegacyNamespaceAccess
	}
)

func (m BadgerManager) ExtractPrefix(input string) (types.Prefix, bool) {
	tokens := strings.Split(input, ":")
	if len(tokens) == 2 {
		return types.Prefix(tokens[0]), true
	}
	return "", false
}

func (m BadgerManager) ExpandPrefix(input types.Prefix) (types.URI, error) {
	return m.store.LookupNamespaceExpansion(input)
}

//ExtractNamespaceURI split given URI into namespace and value
//  returns (
//    namespaceURI with trailing slash,
//    value (last path element),
//    success flag
//   )
func (m BadgerManager) ExtractNamespaceURI(input string) (types.URI, string, bool) {
	if strings.HasPrefix(input, "http") {
		cutPosition := strings.LastIndex(input, "/") + 1
		return types.URI(input[:cutPosition]), input[cutPosition:], true
	}
	return "", "", false
}

func (m BadgerManager) GetNamespacePrefix(input types.URI) (types.Prefix, error) {
	return m.store.LookupExpansionPrefix(input)
}

func NewManager(s store.LegacyNamespaceAccess) Manager {
	return BadgerManager{s}
}
