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

// Package matcher providers implementations for most basic AMQ match strategies.
package matcher

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/canni/paperboymq/amq"
)

// Direct matcher matches if both, message routing key and binding key are equal.
var Direct = New("direct", directMatchFunc)

// Fanout matcher always matches
var Fanout = New("fanout", fanoutMatchFunc)

// Topic matcher matches when routing key matches binding pattern, see AMQP
// specification for detailed information. There is no need to rewrite all
// rules here.
var Topic = New("topic", topicMatchFunc)

func directMatchFunc(msg amq.Message, binding *amq.Binding) bool {
	return msg.RoutingKey() == binding.Key
}

func fanoutMatchFunc(msg amq.Message, binding *amq.Binding) bool {
	return true
}

var patternCache = struct {
	cache map[string]*regexp.Regexp
	sync.RWMutex
}{
	cache: make(map[string]*regexp.Regexp),
}

func topicMatchFunc(msg amq.Message, binding *amq.Binding) bool {
	patternCache.RLock()
	defer patternCache.RUnlock()

	if matcher, found := patternCache.cache[binding.Key]; found {
		return matcher.MatchString(msg.RoutingKey())
	} else {
		pattern := strings.Replace(binding.Key, ".", `\.`, -1)
		pattern = strings.Replace(pattern, "*", `[0-9A-z]+`, -1)
		pattern = strings.Replace(pattern, `\.#\.`, `(\.|\.[0-9A-z\.]*\.)`, -1)
		pattern = strings.Replace(pattern, `\.#`, `(\.[0-9A-z\.]*)?`, -1)
		pattern = strings.Replace(pattern, `#\.`, `([0-9A-z\.]*\.)?`, -1)
		pattern = strings.Replace(pattern, "#", `[0-9A-z\.]*`, -1)

		matcher := regexp.MustCompile(fmt.Sprintf("^%s$", pattern))

		go func() {
			patternCache.Lock()
			defer patternCache.Unlock()

			patternCache.cache[binding.Key] = matcher
		}()

		return matcher.MatchString(msg.RoutingKey())
	}
}

// New returns Matcher implementation that supports comparision through equality
// operator `==`
func New(name string, fn func(amq.Message, *amq.Binding) bool) amq.Matcher {
	return &matcherImpl{
		name: name,
		fn:   &fn,
	}
}

type matcherImpl struct {
	name string
	fn   *func(amq.Message, *amq.Binding) bool
}

func (self *matcherImpl) Matches(msg amq.Message, binding *amq.Binding) bool {
	return (*self.fn)(msg, binding)
}

func (self matcherImpl) String() string {
	return self.name
}
