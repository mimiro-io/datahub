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
	"strings"
)

// Entity data structure
type Entity struct {
	ID         string                 `json:"id"`
	InternalID uint64                 `json:"internalId"`
	Recorded   uint64                 `json:"recorded"`
	IsDeleted  bool                   `json:"deleted"`
	References map[string]interface{} `json:"refs"`
	Properties map[string]interface{} `json:"props"`
}

// NewEntity Create a new entity with global uri and internal resource id
func NewEntity(ID string, internalID uint64) *Entity {
	e := Entity{}
	e.ID = ID
	e.InternalID = internalID
	e.Properties = make(map[string]interface{})
	e.References = make(map[string]interface{})
	return &e
}

func NewEntityFromMap(data map[string]interface{}) *Entity {
	e := Entity{}
	e.ID = data["id"].(string)
	e.Properties = data["props"].(map[string]interface{})
	e.References = data["refs"].(map[string]interface{})
	return &e
}

func (e *Entity) ExpandIdentifiers(store *Store) error {
	newProps := make(map[string]interface{})
	newRefs := make(map[string]interface{})
	var err error

	e.ID, err = store.ExpandCurie(e.ID)
	if err != nil {
		return err
	}

	for n, p := range e.Properties {
		expanded, err := store.ExpandCurie(n)
		if err != nil {
			return err
		}
		newProps[expanded] = p
	}

	for n, r := range e.References {
		switch v := r.(type) {
		case string:
			expanded, err := store.ExpandCurie(n)
			if err != nil {
				return err
			}
			newRefs[expanded] = v
		case []string:
			refs := make([]string, 0)
			for _, ref := range v {
				expanded, err := store.ExpandCurie(ref)
				if err != nil {
					return err
				}
				refs = append(refs, expanded)
			}
			expanded, err := store.ExpandCurie(n)
			if err != nil {
				return err
			}
			newRefs[expanded] = refs
		}
	}

	e.References = newRefs
	e.Properties = newProps
	return nil
}

// GetName returns a human readable Name
func (e *Entity) GetName() string {
	name := e.GetStringProperty(RdfsLabelUri)

	if name == "" {
		index := strings.LastIndex(e.ID, "/")
		if index > 0 {
			name = e.ID[index+1:]
		} else {
			index = strings.LastIndex(e.ID, "#")
			if index > 0 {
				name = e.ID[index+1:]
			} else {
				name = e.ID
			}
		}
	}

	return name
}

// GetStringProperty returns the string value of the requested property
func (e *Entity) GetStringProperty(propName string) string {
	val := e.Properties[propName]
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	default:
		return ""
	}
}

// GetProperty returns the value of the named property as an interface
func (e *Entity) GetProperty(propName string) interface{} {
	prop := e.Properties[propName]
	switch prop.(type) {
	case map[string]interface{}:
		return NewEntityFromMap(prop.(map[string]interface{}))
	default:
		return prop
	}
}
