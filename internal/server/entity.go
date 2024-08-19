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
	"fmt"
	"reflect"
	"strings"
)

// Entity data structure
type Entity struct {
	References map[string]interface{} `json:"refs"`
	Properties map[string]interface{} `json:"props"`
	ID         string                 `json:"id,omitempty"`
	InternalID uint64                 `json:"internalId,omitempty"`
	Recorded   uint64                 `json:"recorded,omitempty"`
	IsDeleted  bool                   `json:"deleted,omitempty"`
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
	name := e.GetStringProperty(RdfsLabelURI)

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
	switch p := prop.(type) {
	case map[string]interface{}:
		return NewEntityFromMap(p)
	default:
		return prop
	}
}

func IsEntityEqualOld(prevJson []byte, thisJson []byte, prevEntity *Entity, thisEntity *Entity) bool {
	if !(len(prevJson) == len(thisJson)) {
		return false
	}
	if !reflect.DeepEqual(prevEntity.References, thisEntity.References) {
		return false
	}
	return reflect.DeepEqual(prevEntity.Properties, thisEntity.Properties)
}

func toMap(in any, tag string) (map[string]any, error) {
	out := make(map[string]any)

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("toMap only accepts structs; got %T", v)
	}

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		// gets us a StructField
		fi := typ.Field(i)
		if tagv := fi.Tag.Get(tag); tagv != "" {
			val := v.Field(i).Interface()
			// set key of map to value in struct field
			t := strings.SplitN(tagv, ",", 2)
			if len(t) > 1 && t[1] == "omitempty" { //&& reflect.DeepEqual(val, reflect.Zero(v.Field(i).Type()).Interface()) {
				if t[0] == "deleted" && val == false {
					continue
				}
				if t[0] == "recorded" && val == uint64(0) {
					continue
				}
			}
			if t[0] == "refs" {
				refs := val.(map[string]interface{})
				for k, v := range refs {
					refs[k], _ = toJsonValue(v)
				}
			}
			if t[0] == "props" {
				props := val.(map[string]interface{})
				for k, v := range props {
					props[k], _ = toJsonValue(v)
				}
			}
			out[t[0]], _ = toJsonValue(val)
		}
	}
	return out, nil
}

func toJsonValue(value any) (any, reflect.Type) {
	vt := reflect.ValueOf(value)
	if vt.Kind() == reflect.Ptr {
		vt = vt.Elem()
	}

	if vt.Kind() == reflect.Slice {
		v2 := make([]any, vt.Len(), vt.Cap())
		for i := 0; i < vt.Len(); i++ {
			v2[i], _ = toJsonValue(vt.Index(i).Interface())
		}
		value = v2
	}

	// TODO: https://open.mimiro.io/specifications/uda/latest.html#the-entity-graph-data-model
	// maps as properties are now allowed according to the spec, so we are not making an effort to
	// align the map values with json types. This is a potential issue since nothing
	// in transforms prevents putting maps in properties (but the spec does not allow it).

	//if vt.Kind() == reflect.Map {
	//v2 := make(map[string]any)
	//for _, k := range vt.MapKeys() {
	//	v2[k.String()], _ = toJsonValue(vt.MapIndex(k).Interface())
	//}
	//value = v2
	//}
	if vt.Kind() == reflect.Struct {
		v2, _ := toMap(value, "json")
		vt = reflect.ValueOf(v2)
		value = v2
	}

	switch v := value.(type) {
	case map[string]interface{}:
		return v, vt.Type()
	case []interface{}:
		return v, vt.Type()
	case string:
		return v, vt.Type()
	case int:
		return float64(v), vt.Type()
	case int8:
		return float64(v), vt.Type()
	case int16:
		return float64(v), vt.Type()
	case int32:
		return float64(v), vt.Type()
	case int64:
		return float64(v), vt.Type()
	case uint:
		return float64(v), vt.Type()
	case uint8:
		return float64(v), vt.Type()
	case uint16:
		return float64(v), vt.Type()
	case uint32:
		return float64(v), vt.Type()
	case uint64:
		return float64(v), vt.Type()
	case float32:
		return float64(v), vt.Type()
	case []map[string]any:
		return v, vt.Type()
	default:
		if vt.IsValid() {
			return v, vt.Type()
		}
		return v, reflect.TypeOf(0)
	}
}
func IsEntityEqual(prevJson []byte, thisJson []byte, prevEntity *Entity, thisEntity *Entity) bool {
	if !(len(prevJson) == len(thisJson)) {
		return false
	}

	// assuming that the length check is enough to determine that refs and props have the same keys
	// it is theoretically possible to have the same json length with different keys ... consider matching keys in both objects as well.
	for i, v := range prevEntity.References {
		thisVal, ok := thisEntity.References[i]
		if !ok {
			return false
		}
		v1, t1 := toJsonValue(v)
		v2, t2 := toJsonValue(thisVal)
		if t1.Kind() == reflect.Slice || t2.Kind() == reflect.Slice {
			if !reflect.DeepEqual(v1, v2) {
				return false
			}
		} else if v1 != v2 {
			return false
		}
	}
	for k, v := range prevEntity.Properties {
		v1, t1 := toJsonValue(v)
		thisVal, ok := thisEntity.Properties[k]
		if !ok {
			return false
		}
		v2, t2 := toJsonValue(thisVal)
		if t1.Kind() == reflect.Slice || t1.Kind() == reflect.Map || t2.Kind() == reflect.Slice || t2.Kind() == reflect.Map {
			if !reflect.DeepEqual(v1, v2) {
				return false
			}
		} else if v1 != v2 {
			return false
		}
	}
	return true
}
