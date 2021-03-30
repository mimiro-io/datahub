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
	"errors"
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
	return fmt.Sprintf("%s", e.msg)
}

// we define these errors to prevent leaking of internal details on the api
var (
	AttemptStoreEntitiesErr = errors.New("failed when attempting to store entities")
	SinceParseErr           = errors.New("since should be an integer number")
	HttpBodyMissingErr      = errors.New("body is missing or could not read")
	HttpJobParsingErr       = errors.New("failed at parsing the job definition")
	HttpJobSchedulingErr    = errors.New("failed at scheduling the job definition")
	HttpJsonParsingErr      = errors.New("failed parsing the json body")
	HttpContentStoreErr     = errors.New("failed updating the content")
	HttpQueryParamErr       = errors.New("one or more of the query parameters failed its validation")
	HttpGenericErr          = errors.New("internal failure")
	HttpFullsyncRunning     = errors.New("cannot start full sync while another full sync is in process")
)
