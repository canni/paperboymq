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

package queue

import (
	"sync"
	"testing"

	"github.com/canni/paperboymq/amq"
)

func TestQueueHandler_NewHasZeroLength(t *testing.T) {
	q := NewQueueHandler()

	if q.Len() != 0 {
		t.Error("Unexpected non-empty queue")
	}
}

func TestQueueHandler_PeekOnEmptyQueuePanics(t *testing.T) {
	defer func() {
		if err := recover(); err != "queue: Peek() called on empty queue" {
			t.Error("Expected panic not fired")
		}
	}()

	q := NewQueueHandler()
	_ = q.Peek()
}

func TestQueueHandler_RemoveFromEmptyQueuePanics(t *testing.T) {
	defer func() {
		if err := recover(); err != "queue: Remove() called on empty queue" {
			t.Error("Expected panic not fired", err)
		}
	}()

	q := NewQueueHandler()
	q.Remove()
}

func TestQueueHandler_QueuesMessages(t *testing.T) {
	q := NewQueueHandler()

	for i := 0; i < 100; i++ {
		q.Add(testMsg{})
	}

	if q.Len() != 100 {
		t.Errorf("Invalid number of meesages, expected %d got %d", 100, q.Len())
	}
}

func TestQueueHandler_ReturnMessagesInCorrectOrder(t *testing.T) {
	q := NewQueueHandler()

	for i := 0; i < 100; i++ {
		q.Add(testMsg{
			priority: uint8(i),
		})
	}

	for i := 0; i < 100; i++ {
		msg := q.Peek()
		if msg.Priority() != uint8(i) {
			t.Errorf("Invalid message priority, expected %d got %d", i, msg.Priority())
		}

		q.Remove()
	}
}

func TestQueueHandler_AfterReturningAllMessagesHasZeroLength(t *testing.T) {
	q := NewQueueHandler()

	for i := 0; i < 100; i++ {
		q.Add(testMsg{})
	}

	for i := 0; i < 100; i++ {
		_ = q.Peek()
		q.Remove()
	}

	if q.Len() != 0 {
		t.Error("Unexpected non-empty queue")
	}
}

func TestQueueHandler_AddRemoveCycle(t *testing.T) {
	q := NewQueueHandler()

	for i := 0; i < 50; i++ {
		q.Add(testMsg{
			priority: uint8(i),
		})
	}
	if q.Len() != 50 {
		t.Error("Unexpected queue length, expected %d got %d", 50, q.Len())
	}

	for i := 0; i < 25; i++ {
		msg := q.Peek()
		if msg.Priority() != uint8(i) {
			t.Errorf("Invalid message priority, expected %d got %d", i, msg.Priority())
		}

		q.Remove()
	}
	if q.Len() != 25 {
		t.Error("Unexpected queue length, expected %d got %d", 25, q.Len())
	}

	for i := 50; i < 100; i++ {
		q.Add(testMsg{
			priority: uint8(i),
		})
	}
	if q.Len() != 75 {
		t.Error("Unexpected queue length, expected %d got %d", 75, q.Len())
	}

	for i := 25; i < 100; i++ {
		msg := q.Peek()
		if msg.Priority() != uint8(i) {
			t.Errorf("Invalid message priority, expected %d got %d", i, msg.Priority())
		}

		q.Remove()
	}
	if q.Len() != 0 {
		t.Error("Unexpected queue length, expected %d got %d", 0, q.Len())
	}
}

type testMsg struct {
	headers    amq.Headers
	routingKey string
	priority   uint8
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

func (self testMsg) Body() []byte {
	return self.body
}

type countingConsumer struct {
	mu         sync.RWMutex
	callsCount int
}
