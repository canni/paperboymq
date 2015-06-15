/*
Copyright 2015 Dariusz Górecki <darek.krk@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package amq provides high-level message broker building blocks.
package amq

import (
	"time"
)

// Headers type is a mapping of string header names to values.
type Headers map[string]interface{}

// Message interface is a core of this implementation,
// the methods defined on this interface are directly used within
// this package.
type Message interface {
	Headers() Headers
	RoutingKey() string
	Priority() uint8
	Timestamp() time.Time
	Body() []byte
}

// MessageConsumer is an interface representing entity capable of consuming,
// receiving or accumulating Messages.
//
// MessageConsumer interface implementors MUST support equality check through
// `==` operator.
type MessageConsumer interface {
	Consume(Message)
}

// RoundRobinDispatcher is an interface representing entity capable
// of dispatching messages to set of MessageConsumers in a round-robin fashion.
type RoundRobinDispatcher interface {
	Subscribe(MessageConsumer) error
	Unsubscribe(MessageConsumer) error
	Subscriptions() []MessageConsumer
}

// Binding is an interface representing connection between Message Exchange and
// either another Message Exchange or Message Queue.
//
// Binding interface implementors MUST support equality check through
// `==` operator.
type Binding interface {
	BindingKey() string
	Consumer() MessageConsumer
}

// MessagePublisher is an interface representing entity capable of directing
// messages to specified binding.
type MessagePublisher interface {
	BindTo(Binding) error
	UnbindFrom(Binding) error
}

// Matcher is an interface witch will be used by Exchange implementation
// to decide whatever to pass message over particular binding.
type Matcher interface {
	Matches(Message, Binding) bool
}

// MatchFunc type implements Matcher interface and is simple wrapper for function
// matchers.
type MatchFunc func(Message, Binding) bool

func (self MatchFunc) Matches(msg Message, binding Binding) bool {
	return self(msg, binding)
}

type binding struct {
	key      string
	consumer MessageConsumer
}

func NewBinding(bindingKey string, consumer MessageConsumer) Binding {
	return &binding{
		key:      bindingKey,
		consumer: consumer,
	}
}

func (self *binding) BindingKey() string {
	return self.key
}

func (self *binding) Consumer() MessageConsumer {
	return self.consumer
}
