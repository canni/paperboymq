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
)

var (
	ErrConsumerAlreadySubscribed = errors.New("Round robin: Consumer already subscribed")
	ErrConsumerNotFound          = errors.New("Round robin: Consumer not found")
)

type consumerRoundRobin struct {
	ring      *consumersRing
	consumers map[MessageConsumer]struct{}
}

func newRoundRobinHandler() *consumerRoundRobin {
	return &consumerRoundRobin{
		consumers: make(map[MessageConsumer]struct{}),
	}
}

func (self *consumerRoundRobin) Len() int {
	return len(self.consumers)
}

func (self *consumerRoundRobin) Add(consumer MessageConsumer) error {
	if _, found := self.consumers[consumer]; found {
		return ErrConsumerAlreadySubscribed
	}

	self.ring = self.ring.add(consumer)
	self.consumers[consumer] = struct{}{}
	return nil
}

func (self *consumerRoundRobin) Remove(consumer MessageConsumer) error {
	if _, found := self.consumers[consumer]; !found {
		return ErrConsumerNotFound
	}

	self.ring = self.ring.remove(consumer)
	delete(self.consumers, consumer)
	return nil
}

func (self *consumerRoundRobin) Next() MessageConsumer {
	defer func() { self.ring = self.ring.next }()
	return self.ring.consumer
}

type consumersRing struct {
	prev, next *consumersRing
	consumer   MessageConsumer
}

func (self *consumersRing) add(consumer MessageConsumer) *consumersRing {
	elem := &consumersRing{
		consumer: consumer,
	}

	if self == nil {
		elem.prev = elem
		elem.next = elem
		return elem
	}

	elem.prev = self.prev
	elem.next = self

	self.prev.next = elem
	self.prev = elem

	return self
}

func (self *consumersRing) remove(consumer MessageConsumer) *consumersRing {
	if self == nil {
		return nil
	}

	// find element whose next ring element contains consumer to remove
	current := self
	for current.next.consumer != consumer {
		current = current.next
	}

	// Move next pointer
	current.next = current.next.next
	// Fix previous pointer of next element
	current.next.prev = current

	if self.consumer == consumer {
		return self.next
	}

	return self
}
