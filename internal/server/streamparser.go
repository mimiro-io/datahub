// Copyright 2021 MIMIRO AS
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

package server

import (
	"encoding/json"
	"errors"
	"io"
)

type EntityStreamParser struct {
	store                 *Store
	localNamespaces       map[string]string
	localPropertyMappings map[string]string
	processingContext     bool
}

func NewEntityStreamParser(store *Store) *EntityStreamParser {
	esp := &EntityStreamParser{}
	esp.store = store
	esp.localNamespaces = make(map[string]string)
	esp.localPropertyMappings = make(map[string]string)
	return esp
}

/*
{
	"@context" : {

	},
	"ds1" : [

	],
	"ds2" : [

	]
}
*/

func (esp *EntityStreamParser) ParseTransaction(reader io.Reader) (*Transaction, error) {
	txn := &Transaction{}
	txn.DatasetEntities = make(map[string][]*Entity)

	decoder := json.NewDecoder(reader)

	t, err := decoder.Token()
	if err != nil {
		return nil, errors.New("parsing error: Bad token at start of stream " + err.Error())
	}

	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("parsing error: Expected { at start of transaction json")
	}

	// key @context
	_, err = decoder.Token()
	if err != nil {
		return nil, errors.New("parsing error: Bad token at read @context key " + err.Error())
	}

	context := make(map[string]interface{})
	err = decoder.Decode(&context)
	if err != nil {
		return nil, errors.New("parsing error: Unable to decode context " + err.Error())
	}

	for k, v := range context["namespaces"].(map[string]interface{}) {
		esp.localNamespaces[k] = v.(string)
	}

	for {
		t, _ = decoder.Token()
		delimVal, isDelim := t.(json.Delim)
		if isDelim {
			if delimVal.String() == "}" {
				break
			} else {
				return nil, errors.New("parsing error: Unexpected delimiter: " + delimVal.String())
			}
		} else {
			datasetName := t.(string)

			// read [
			t, err = decoder.Token()
			if err != nil {
				return nil, errors.New("parsing error: Unable to read next token " + err.Error())
			}
			delimVal, isDelim := t.(json.Delim)
			if !isDelim && delimVal.String() != "[" {
				return nil, errors.New("parsing error: Unexpected delimiter - expected [ but got : " + delimVal.String())
			}

			entities := make([]*Entity, 0)
			for {
				t, err = decoder.Token() // starting { or ending ]
				if err != nil {
					return nil, errors.New("parsing error: Unable to read next token " + err.Error())
				}

				delimVal, isDelim := t.(json.Delim)
				if isDelim && delimVal.String() == "{" {
					e, err := esp.parseEntity(decoder)
					if err != nil {
						return nil, errors.New("parsing error: Unable to parse entity: " + err.Error())
					}
					entities = append(entities, e)
				} else if isDelim && delimVal.String() == "]" {
					break
				}
			}

			txn.DatasetEntities[datasetName] = entities
		}
	}

	return txn, nil
}

func (esp *EntityStreamParser) ParseStream(reader io.Reader, emitEntity func(*Entity) error) error {
	decoder := json.NewDecoder(reader)

	// expect start of array
	t, err := decoder.Token()
	if err != nil {
		return errors.New("parsing error: Bad token at start of stream " + err.Error())
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return errors.New("parsing error: Expected [ at start of document")
	}

	// decode context object
	context := make(map[string]interface{})
	err = decoder.Decode(&context)
	if err != nil {
		return errors.New("parsing error: Unable to decode context " + err.Error())
	}

	if context["id"] == "@context" {
		for k, v := range context["namespaces"].(map[string]interface{}) {
			esp.localNamespaces[k] = v.(string)
		}
	} else {
		return errors.New("first entity in array must be a context")
	}

	for {
		t, err = decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return errors.New("parsing error: Unable to read next token " + err.Error())
			}
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '{' {
				e, err := esp.parseEntity(decoder)
				if err != nil {
					return errors.New("parsing error: Unable to parse entity: " + err.Error())
				}
				err = emitEntity(e)
				if err != nil {
					return err
				}
			} else if v == ']' {
				// done
				break
			}
		default:
			return errors.New("parsing error: unexpected value in entity array")
		}
	}

	return nil
}

func (esp *EntityStreamParser) parseEntity(decoder *json.Decoder) (*Entity, error) {
	e := &Entity{}
	e.Properties = make(map[string]interface{})
	e.References = make(map[string]interface{})
	isContinuation := false
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token " + err.Error())
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '}' {
				return e, nil
			}
		case string:
			switch v {
			case "id":
				val, err2 := decoder.Token()
				if err2 != nil {
					return nil, errors.New("unable to read token of id value " + err2.Error())
				}

				if val.(string) == "@continuation" {
					e.ID = "@continuation"
					isContinuation = true
				} else {
					nsID, err2 := esp.store.GetNamespacedIdentifier(val.(string), esp.localNamespaces)
					if err2 != nil {
						return nil, err2
					}
					e.ID = nsID
				}
			case "recorded":
				val, err2 := decoder.Token()
				if err2 != nil {
					return nil, errors.New("unable to read token of recorded value " + err2.Error())
				}
				e.Recorded = uint64(val.(float64))

			case "deleted":
				val, err2 := decoder.Token()
				if err2 != nil {
					return nil, errors.New("unable to read token of deleted value " + err2.Error())
				}
				e.IsDeleted = val.(bool)

			case "props":
				e.Properties, err = esp.parseProperties(decoder)
				if err != nil {
					return nil, errors.New("unable to parse properties " + err.Error())
				}
			case "refs":
				e.References, err = esp.parseReferences(decoder)
				if err != nil {
					return nil, errors.New("unable to parse references " + err.Error())
				}
			case "token":
				if !isContinuation {
					return nil, errors.New("token property found but not a continuation entity")
				}
				val, err := decoder.Token()
				if err != nil {
					return nil, errors.New("unable to read continuation token value " + err.Error())
				}
				e.Properties = make(map[string]interface{})
				e.Properties["token"] = val
			default:
				// log named property
				// read value
				_, err := decoder.Token()
				if err != nil {
					return nil, errors.New("unable to parse value of unknown key: " + v + err.Error())
				}
			}
		default:
			return nil, errors.New("unexpected value in entity")
		}
	}
}

func (esp *EntityStreamParser) parseReferences(decoder *json.Decoder) (map[string]interface{}, error) {
	refs := make(map[string]interface{})

	_, err := decoder.Token()
	if err != nil {
		return nil, errors.New("unable to read token of at start of references " + err.Error())
	}

	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token in parse references " + err.Error())
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '}' {
				return refs, nil
			}
		case string:
			val, err := esp.parseRefValue(decoder)
			if err != nil {
				return nil, errors.New("unable to parse value of reference key " + v)
			}

			propName := esp.localPropertyMappings[v]
			if propName == "" {
				propName, err = esp.store.GetNamespacedIdentifier(v, esp.localNamespaces)
				if err != nil {
					return nil, err
				}
				esp.localPropertyMappings[v] = propName
			}
			refs[propName] = val
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseProperties(decoder *json.Decoder) (map[string]interface{}, error) {
	props := make(map[string]interface{})

	_, err := decoder.Token()
	if err != nil {
		return nil, errors.New("unable to read token of at start of properties " + err.Error())
	}

	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token in parse properties " + err.Error())
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '}' {
				return props, nil
			}
		case string:
			val, err := esp.parseValue(decoder)
			if err != nil {
				return nil, errors.New("unable to parse property value of key " + v + "err: " + err.Error())
			}

			if val != nil { // basically if both error is nil, and value is nil, we drop the field
				propName := esp.localPropertyMappings[v]
				if propName == "" {
					propName, err = esp.store.GetNamespacedIdentifier(v, esp.localNamespaces)
					if err != nil {
						return nil, err
					}
					esp.localPropertyMappings[v] = propName
				}
				props[propName] = val
			}
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseRefValue(decoder *json.Decoder) (interface{}, error) {
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token in parse value " + err.Error())
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '[' {
				return esp.parseRefArray(decoder)
			}
		case string:
			nsRef, err := esp.store.GetNamespacedIdentifier(v, esp.localNamespaces)
			if err != nil {
				return nil, err
			}
			return nsRef, nil
		default:
			return nil, errors.New("unknown token in parse ref value")
		}
	}
}

func (esp *EntityStreamParser) parseRefArray(decoder *json.Decoder) ([]string, error) {
	array := make([]string, 0)
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token in parse ref array " + err.Error())
		}

		switch v := t.(type) {
		case json.Delim:
			if v == ']' {
				return array, nil
			}
		case string:
			nsRef, err := esp.store.GetNamespacedIdentifier(v, esp.localNamespaces)
			if err != nil {
				return nil, err
			}
			array = append(array, nsRef)
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseArray(decoder *json.Decoder) ([]interface{}, error) {
	array := make([]interface{}, 0)
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token in parse array " + err.Error())
		}

		switch v := t.(type) {
		case json.Delim:
			switch v {
			case '{':
				r, err := esp.parseEntity(decoder)
				if err != nil {
					return nil, errors.New("unable to parse array " + err.Error())
				}
				array = append(array, r)
			case ']':
				return array, nil
			case '[':
				r, err := esp.parseArray(decoder)
				if err != nil {
					return nil, errors.New("unable to parse array " + err.Error())
				}
				array = append(array, r)
			}
		case string:
			array = append(array, v)
		case int:
			array = append(array, v)
		case float64:
			array = append(array, v)
		case bool:
			array = append(array, v)
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseValue(decoder *json.Decoder) (interface{}, error) {
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, errors.New("unable to read token in parse value " + err.Error())
		}

		if t == nil {
			// there is a good chance that we got a null value, and we need to handle that
			return nil, nil
		}

		switch v := t.(type) {
		case json.Delim:
			switch v {
			case '{':
				return esp.parseEntity(decoder)
			case '[':
				return esp.parseArray(decoder)
			}
		case string:
			return v, nil
		case int:
			return v, nil
		case float64:
			return v, nil
		case bool:
			return v, nil
		default:
			return nil, errors.New("unknown token in parse value")
		}
	}
}
