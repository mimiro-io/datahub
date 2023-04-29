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
)

// StorageError custom error from storage
type StorageError struct {
	msg        string
	innerError error
}

// NewStorageError Creates a new storage error with optional innerError
func NewStorageError(msg string, innerError error) *StorageError {
	e := StorageError{}
	e.msg = msg
	e.innerError = innerError
	return &e
}

func (e *StorageError) Error() string {
	return e.msg
}

// we define these errors to prevent leaking of internal details on the api
var (
	AttemptStoreEntitiesErr = func(detail error) error { return fmt.Errorf("failed when attempting to store entities: %w", detail) }
	SinceParseErr           = func(detail error) error { return fmt.Errorf("since should be an integer number: %w", detail) }
	HTTPBodyMissingErr      = func(detail error) error { return fmt.Errorf("body is missing or could not read: %w", detail) }
	HTTPJobParsingErr       = func(detail error) error { return fmt.Errorf("failed at parsing the job definition: %w", detail) }
	HTTPJobSchedulingErr    = func(detail error) error { return fmt.Errorf("failed at scheduling the job definition: %w", detail) }
	HTTPJsonParsingErr      = func(detail error) error { return fmt.Errorf("failed parsing the json body: %w", detail) }
	HTTPContentStoreErr     = func(detail error) error { return fmt.Errorf("failed updating the content: %w", detail) }
	HTTPQueryParamErr       = func(detail error) error {
		return fmt.Errorf("one or more of the query parameters failed its validation: %w", detail)
	}
	HTTPGenericErr  = func(detail error) error { return fmt.Errorf("internal failure: %w", detail) }
	HTTPFullsyncErr = func(detail error) error {
		return fmt.Errorf("an error occured trying to start or update a full sync: %w", detail)
	}
)
