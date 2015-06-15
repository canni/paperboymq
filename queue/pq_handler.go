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
	"container/heap"

	"github.com/canni/paperboymq/amq"
)

type heapImpl []amq.Message

func (self heapImpl) Len() int {
	return len(self)
}

func (self heapImpl) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self heapImpl) Less(i, j int) bool {
	l, r := self[i], self[j]

	if l.Priority() == r.Priority() {
		return l.Timestamp().Before(r.Timestamp())
	}

	return l.Priority() > r.Priority()
}

func (self *heapImpl) Push(v interface{}) {
	*self = append(*self, v.(amq.Message))
}

func (self *heapImpl) Pop() interface{} {
	old := *self
	n := len(old)
	v := old[n-1]
	*self = old[:n-1]

	return v
}

type pqHandler struct {
	*heapImpl
}

func NewPQHandler() amq.QueueHandler {
	return pqHandler{&heapImpl{}}
}

func (self pqHandler) Len() int {
	return self.heapImpl.Len()
}

func (self pqHandler) Add(msg amq.Message) {
	heap.Push(self.heapImpl, msg)
}

func (self pqHandler) Peek() amq.Message {
	if self.heapImpl.Len() == 0 {
		panic("queue: Peek() called on empty queue")
	}

	return (*self.heapImpl)[0]
}

func (self pqHandler) Remove() {
	if self.heapImpl.Len() == 0 {
		panic("queue: Remove() called on empty queue")
	}

	heap.Pop(self.heapImpl)
}
