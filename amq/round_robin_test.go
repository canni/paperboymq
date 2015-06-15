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
	"testing"
)

func TestRoundRobinHandler_EmptyHandlerHasZeroLength(t *testing.T) {
	rr := newRoundRobinHandler()

	if rr.Len() != 0 {
		t.Error("Unexpected non-empty handler")
	}
}

func TestRoundRobinHandler_NextOnEmptyHandlerPanics(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("Expected panic not fired")
		}
	}()

	rr := newRoundRobinHandler()
	_ = rr.Next()
}

func TestRoundRobinHandler_CantAddSameConsumerMultipleTimes(t *testing.T) {
	rr := newRoundRobinHandler()
	c := make(testConsumer)

	err := rr.Add(c)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	err = rr.Add(c)
	if err != ErrConsumerAlreadySubscribed {
		t.Error("Expected ErrConsumerAlreadySubscribed not returned")
	}
}

func TestRoundRobinHandler_CantRemoveSameConsumerMultipleTimes(t *testing.T) {
	rr := newRoundRobinHandler()
	c := make(testConsumer)

	err := rr.Add(c)
	err = rr.Remove(c)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	err = rr.Remove(c)
	if err != ErrConsumerNotFound {
		t.Error("Expected ErrConsumerAlreadySubscribed not returned")
	}
}

func TestRoundRobinHandler_AddRemoveCycle(t *testing.T) {
	rr := newRoundRobinHandler()
	c1 := make(testConsumer)
	c2 := make(testConsumer)
	c3 := make(testConsumer)

	rr.Add(c1)
	if rr.Len() != 1 {
		t.Errorf("Unexpected round-robin ring length, expected %d got %d", 1, rr.Len())
	}

	rr.Add(c2)
	if rr.Len() != 2 {
		t.Errorf("Unexpected round-robin ring length, expected %d got %d", 2, rr.Len())
	}

	rr.Remove(c2)
	if rr.Len() != 1 {
		t.Errorf("Unexpected round-robin ring length, expected %d got %d", 1, rr.Len())
	}

	rr.Add(c3)
	if rr.Len() != 2 {
		t.Errorf("Unexpected round-robin ring length, expected %d got %d", 2, rr.Len())
	}

	rr.Remove(c3)
	if rr.Len() != 1 {
		t.Errorf("Unexpected round-robin ring length, expected %d got %d", 1, rr.Len())
	}

	rr.Remove(c1)
	if rr.Len() != 0 {
		t.Errorf("Unexpected round-robin ring length, expected %d got %d", 1, rr.Len())
	}
}

func TestRoundRobinHandler_SingleConsumerIsReturnedAlwaysFromNext(t *testing.T) {
	rr := newRoundRobinHandler()
	c := make(testConsumer)

	rr.Add(c)

	for i := 0; i < 10; i++ {
		if result := rr.Next(); result != c {
			t.Error("Round-Robin returned unexpected consumer")
		}
	}
}

func TestRoundRobinHandler_CyclesThroughConsumers(t *testing.T) {
	rr := newRoundRobinHandler()

	// Internal test that we can distinguish consumers with equality op
	if make(testConsumer) == make(testConsumer) {
		t.Error("End of the World!")
	}

	consumers := []MessageConsumer{make(testConsumer), make(testConsumer), make(testConsumer)}
	for _, c := range consumers {
		rr.Add(c)
	}

	for i := 0; i < 10; i++ {
		if result := rr.Next(); result != consumers[i%3] {
			t.Error("Round-Robin returned unexpected consumer")
		}
	}
}

func TestRoundRobinHandler_WeCanRemoveFirstConsumer(t *testing.T) {
	rr := newRoundRobinHandler()

	// Internal test that we can distinguish consumers with equality op
	if make(testConsumer) == make(testConsumer) {
		t.Error("End of the World!")
	}

	consumers := []MessageConsumer{make(testConsumer), make(testConsumer), make(testConsumer)}
	for _, c := range consumers {
		rr.Add(c)
	}

	rr.Remove(consumers[0])

	newRing := consumers[1:]

	for i := 0; i < 10; i++ {
		if result := rr.Next(); result != newRing[i%2] {
			t.Error("Round-Robin returned unexpected consumer")
		}
	}
}

func TestRoundRobinHandler_WeCanRemoveMiddleConsumer(t *testing.T) {
	rr := newRoundRobinHandler()

	// Internal test that we can distinguish consumers with equality op
	if make(testConsumer) == make(testConsumer) {
		t.Error("End of the World!")
	}

	consumers := []MessageConsumer{make(testConsumer), make(testConsumer), make(testConsumer)}
	for _, c := range consumers {
		rr.Add(c)
	}

	rr.Remove(consumers[1])

	newRing := []MessageConsumer{consumers[0], consumers[2]}

	for i := 0; i < 10; i++ {
		if result := rr.Next(); result != newRing[i%2] {
			t.Error("Round-Robin returned unexpected consumer")
		}
	}
}

func TestRoundRobinHandler_WeCanRemoveLastConsumer(t *testing.T) {
	rr := newRoundRobinHandler()

	// Internal test that we can distinguish consumers with equality op
	if make(testConsumer) == make(testConsumer) {
		t.Error("End of the World!")
	}

	consumers := []MessageConsumer{make(testConsumer), make(testConsumer), make(testConsumer)}
	for _, c := range consumers {
		rr.Add(c)
	}

	rr.Remove(consumers[2])

	newRing := consumers[:2]

	for i := 0; i < 10; i++ {
		if result := rr.Next(); result != newRing[i%2] {
			t.Error("Round-Robin returned unexpected consumer")
		}
	}
}

type testConsumer chan Message

func (self testConsumer) Consume(msg Message) {
	self <- msg
}
