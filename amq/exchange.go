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

package amq

import (
	"errors"
	"sync"
)

var (
	ErrAlreadyBound    = errors.New("Exchange: Already bound to this binding")
	ErrBindingNotFound = errors.New("Exchange: Binding not found")
)

// Exchange is a base AMQ entity, it supports goroutine-safe concurrent access.
//
// Exchange needs to be initialized by calling NewExchange() with Matcher
// implementation.
//
// Exchange delivers messages to bound bindings based on result of Matcher.Matches()
// call. Consumer bound via multiple bindings, will receive message only once.
type Exchange struct {
	matcher   Matcher
	consumers map[Binding]MessageConsumer
	mu        sync.RWMutex
}

func NewExchange(matcher Matcher) *Exchange {
	return &Exchange{
		matcher:   matcher,
		consumers: make(map[Binding]MessageConsumer),
	}
}

func (self *Exchange) Consume(msg Message) {
	self.mu.RLock()
	defer self.mu.RUnlock()

	sent := make(map[MessageConsumer]struct{})

	for binding, consumer := range self.consumers {
		if _, alreadySent := sent[consumer]; alreadySent {
			continue
		}

		if self.matcher.Matches(msg, binding) {
			consumer.Consume(msg)
			sent[consumer] = struct{}{}
		}
	}
}

func (self *Exchange) BindTo(binding Binding) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if _, found := self.consumers[binding]; found {
		return ErrAlreadyBound
	}

	self.consumers[binding] = binding.Consumer()
	return nil
}

func (self *Exchange) UnbindFrom(binding Binding) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if _, found := self.consumers[binding]; !found {
		return ErrBindingNotFound
	}

	delete(self.consumers, binding)
	return nil
}

// Ensure *Exchange implements Consumer interface
var _ MessageConsumer = &Exchange{}

// Ensure *Exchange implements Publisher interface
var _ MessagePublisher = &Exchange{}
