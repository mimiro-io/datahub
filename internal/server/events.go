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

/*
The events are used for internal events to trigger an action when data is stored in a dataset
It can possibly be used for other events as well, if we have any
*/
package server

import (
	"context"
	"fmt"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mustafaturan/bus"
	"github.com/mustafaturan/monoton"
	"github.com/mustafaturan/monoton/sequencer"
	"go.uber.org/zap"
)

type EventBus interface {
	Init(datasets []DatasetName)
	RegisterTopic(ds string)
	UnregisterTopic(ds string)
	SubscribeToDataset(id string, matcher string, f func(e *bus.Event))
	UnsubscribeToDataset(id string)
	Emit(ctx context.Context, topicName string, data interface{})
}

type MEventBus struct {
	Bus    *bus.Bus
	logger *zap.SugaredLogger
}

type NoOp struct {
}

func NewBus(env *conf.Env) (EventBus, error) {
	// configure id generator (it doesn't have to be monoton)
	node := uint64(1)
	initialTime := uint64(1577865600000) // set 2020-01-01 PST as initial time
	m, err := monoton.New(sequencer.NewMillisecond(), node, initialTime)
	if err != nil {
		return nil, err
	}

	// init an id generator
	var idGenerator bus.Next = (*m).Next

	// create a new bus instance
	b, err := bus.NewBus(idGenerator)
	if err != nil {
		return nil, err
	}

	eb := &MEventBus{
		Bus:    b,
		logger: env.Logger.Named("eventbus"),
	}

	return eb, nil
}

// Init adds the list of existing datasets that can be subscribed to.
// Datasets are always prefixed with dataset. to separate them from other
// possible future events
func (eb *MEventBus) Init(datasets []DatasetName) {
	for _, ds := range datasets {
		eb.logger.Infof("Registering topic 'dataset.%s'", ds.Name)
		eb.Bus.RegisterTopics(fmt.Sprintf("dataset.%s", ds.Name))
	}
}

// RegisterTopic registers a topic for publishing. "dataset." is prefixed in front of the topic
func (eb *MEventBus) RegisterTopic(ds string) {
	eb.logger.Infof("Registering topic 'dataset.%s'", ds)
	eb.Bus.RegisterTopics(fmt.Sprintf("dataset.%s", ds))
}

// UnregisterTopic removes a topic from subscription
func (eb *MEventBus) UnregisterTopic(ds string) {
	eb.logger.Infof("Un-registering topic 'dataset.%s'", ds)

	topic := fmt.Sprintf("dataset.%s", ds)
	eb.Bus.DeregisterTopics(topic)
}

// SubscribeToDataset adds a subscription to an already registered topic.
// The id should be unique, the matcher is a regexp to match against the registered topics,
// ie: dataset.*, dataset.sdb.*, dataset.sdb.Animal are all valid registrations.
// f is the func to be called
func (eb *MEventBus) SubscribeToDataset(id string, matcher string, f func(e *bus.Event)) {
	eb.logger.Infof("Registering subscription '%s' with matcher 'dataset.%s'", id, matcher)
	handler := bus.Handler{
		Handle:  f,
		Matcher: "dataset." + matcher,
	}
	eb.Bus.RegisterHandler(id, &handler)
}

// UnsubscribeToDataset removes a dataset subscription
func (eb *MEventBus) UnsubscribeToDataset(id string) {
	eb.logger.Infof("Un-registering subscription '%s'", id)
	eb.Bus.DeregisterHandler(id)
}

// Emit emits an event to the bus
func (eb *MEventBus) Emit(ctx context.Context, topicName string, data interface{}) {
	ctx = context.WithValue(ctx, bus.CtxKeyTxID, "")
	_, err := eb.Bus.Emit(ctx, topicName, data)
	if err != nil {
		eb.logger.Warnf(err.Error())
	}
}

func NoOpBus() EventBus {
	return &NoOp{}
}

func (eb *NoOp) Init(datasets []DatasetName)                                        { /* noop */ }
func (eb *NoOp) RegisterTopic(ds string)                                            { /* noop */ }
func (eb *NoOp) UnregisterTopic(ds string)                                          { /* noop */ }
func (eb *NoOp) SubscribeToDataset(id string, matcher string, f func(e *bus.Event)) { /* noop */ }
func (eb *NoOp) UnsubscribeToDataset(id string)                                     { /* noop */ }
func (eb *NoOp) Emit(ctx context.Context, topicName string, data interface{})       { /* noop */ }
