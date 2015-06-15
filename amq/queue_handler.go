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
	"gopkg.in/eapache/queue.v1"
)

type queueHandler struct {
	q *queue.Queue
}

// NewQueueHandler returns default in-memory message queue implementation.
//
// Default implementation is not goroutine-safe, concurrent access is controlled
// inside high-level Queue type.
func NewQueueHandler() QueueHandler {
	return queueHandler{
		q: queue.New(),
	}
}

// Add enqueues message.
func (self queueHandler) Add(msg Message) {
	self.q.Add(msg)
}

// Peek returns current element at the front of queue.
//
// This method panics if the queue is empty.
func (self queueHandler) Peek() Message {
	return self.q.Peek().(Message)
}

// Remove dequeues element at the front of queue.
//
// This method panics if the queue is empty.
func (self queueHandler) Remove() {
	self.q.Remove()
}

// Len obviously returns lenght of queue
func (self queueHandler) Len() int {
	return self.q.Length()
}
