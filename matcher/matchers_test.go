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

package matcher_test

import (
	"testing"
	"time"

	"github.com/canni/paperboymq/amq"
	"github.com/canni/paperboymq/matcher"
)

func TestDirectMatcher(t *testing.T) {
	var cases = []struct {
		routingKey, bindingKey string
		expected               bool
	}{
		{"", "", true},
		{"key", "key", true},
		{"key.extra", "key.extra", true},
		{"", "key", false},
		{"key", "", false},
		{"key.extra", "key", false},
		{"key", "key.extra", false},
	}

	for i, testCase := range cases {
		msg := testMsg{routingKey: testCase.routingKey}
		binding := amq.NewBinding(testCase.bindingKey, nil)

		if result := matcher.Direct.Matches(msg, binding); result != testCase.expected {
			t.Errorf(
				"case: %d; Direct matcher expected: %t, got: %t, for routing key: %q and binding key: %q",
				i+1,
				testCase.expected,
				result,
				testCase.routingKey,
				testCase.bindingKey,
			)
		}
	}
}

func TestFanoutMatcher(t *testing.T) {
	var cases = []struct {
		routingKey, bindingKey string
		expected               bool
	}{
		{"", "", true},
		{"key", "key", true},
		{"key.extra", "key.extra", true},
		{"", "key", true},
		{"key", "", true},
		{"key.extra", "key", true},
		{"key", "key.extra", true},
	}

	for i, testCase := range cases {
		msg := testMsg{routingKey: testCase.routingKey}
		binding := amq.NewBinding(testCase.bindingKey, nil)

		if result := matcher.Fanout.Matches(msg, binding); result != testCase.expected {
			t.Errorf(
				"case: %d; Fanout matcher expected: %t, got: %t, for routing key: %q and binding key: %q",
				i+1,
				testCase.expected,
				result,
				testCase.routingKey,
				testCase.bindingKey,
			)
		}
	}
}

func TestTopicMatcher(t *testing.T) {
	cases := []struct {
		bindingKey, routingKey string
		expected               bool
	}{
		// Direct matches
		{"", "", true},
		{"key", "key", true},
		{"key.anything", "key.anything", true},
		{"key.anything.", "key.anything.", true},
		{".key.anything", ".key.anything", true},
		{".key.anything.", ".key.anything.", true},
		{".key.anything..", ".key.anything..", true},
		{"..key.anything..", "..key.anything..", true},
		{"..key.anything.", "..key.anything.", true},

		// Direct missmatches
		{"key", "", false},
		{"", "key", false},
		{"anthing", "key", false},
		{".anthing", "key", false},
		{"anthing.", "key", false},
		{".anthing.", "key", false},
		{".anthing.", ".key.", false},

		// Single world wildcard matches
		{"*", "key", true},
		{"*.anything", "key.anything", true},
		{"anything.*", "anything.key", true},
		{"*.anything.*", "key.anything.extra", true},
		{"key.*.extra", "key.anything.extra", true},
		{"key.*.*.extra", "key.anything1.anything2.extra", true},
		{"key.*.extra.*", "key.anything1.extra.anything2", true},

		// Single world wildcard missmatches
		{"*", "key.extra", false},
		{"*.key", "extra", false},
		{"key.*", "extra", false},
		{"key.*.extra", "key.extra", false},

		// Wildcard matches
		{"#", "key", true},
		{"#", "key.extra", true},
		{"key.#", "key", true},
		{"key.#", "key.", true},
		{"key.#", "key.extra", true},
		{"key.#", "key.extra.anything", true},
		{"#.key", "key", true},
		{"#.key", ".key", true},
		{"#.key", "extra.key", true},
		{"#.key", "extra.anything.key", true},
		{"key.#.extra", "key.extra", true},
		{"key.#.extra", "key..extra", true},
		{"key.#.extra", "key.anything.extra", true},
		{"key.#.extra", "key.anything1.anything2.extra", true},

		// Wildcard missmatches
		{"#.key", "somekey", false},
		{"key.#", "keytest", false},
		{"key.#.extra", "keyextra", false},
		{"key.#.extra", "key0extra", false},
		{"key.#.extra", "key.0extra", false},
		{"key.#.extra", "key1.extra", false},
		{"key.#.extra", "key.anything.0extra", false},
		{"key.#.extra", "key1.anything.extra", false},
		{"key.#.extra", "key1.anything.0extra", false},
	}

	for i, testCase := range cases {
		msg := testMsg{routingKey: testCase.routingKey}
		binding := amq.NewBinding(testCase.bindingKey, nil)

		if result := matcher.Topic.Matches(msg, binding); result != testCase.expected {
			t.Errorf(
				"case: %d; Topic matcher expected: %t, got: %t, for routing key: %q and binding key: %q",
				i+1,
				testCase.expected,
				result,
				testCase.routingKey,
				testCase.bindingKey,
			)
		} else {
			// Give a change to build cache
			time.Sleep(1 * time.Microsecond)
		}
	}
}

func TestMatchers_CanBeCompared(t *testing.T) {
	cases := []struct {
		lft, right amq.Matcher
		result     bool
	}{
		{matcher.Direct, matcher.Direct, true},
		{matcher.Fanout, matcher.Fanout, true},
		{matcher.Topic, matcher.Topic, true},

		{matcher.Direct, matcher.Fanout, false},
		{matcher.Direct, matcher.Topic, false},
		{matcher.Fanout, matcher.Direct, false},
		{matcher.Fanout, matcher.Topic, false},
		{matcher.Topic, matcher.Direct, false},
		{matcher.Topic, matcher.Fanout, false},
	}

	for i, testCase := range cases {
		if result := testCase.lft == testCase.right; result != testCase.result {
			t.Errorf(
				"case: %d; Matcher equality test failed, expected %t, got %t",
				i+1,
				testCase.result,
				result,
			)
		}
	}
}

type testMsg struct {
	headers    amq.Headers
	routingKey string
	priority   uint8
	timestamp  time.Time
	body       []byte
}

func (self testMsg) Headers() amq.Headers {
	return self.headers
}

func (self testMsg) RoutingKey() string {
	return self.routingKey
}

func (self testMsg) Priority() uint8 {
	return self.priority
}

func (self testMsg) Timestamp() time.Time {
	return self.timestamp
}

func (self testMsg) Body() []byte {
	return self.body
}
