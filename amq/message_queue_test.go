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
	"sync"
	"testing"
)

func TestEmptyQueueHasZeroLength(t *testing.T) {
	q := NewQueue(NewQueueHandler)

	if q.Len() != 0 {
		t.Errorf("Unexpected queue length")
	}
	q.Close()
}

func TestForceCloseOnEmptyQueue(t *testing.T) {
	// Just to get coverage, there is no way to distinguish close from force close on empty queue
	q := NewQueue(NewQueueHandler)

	if q.Len() != 0 {
		t.Errorf("Unexpected queue length")
	}
	q.ForceClose()
}

func TestQueueWithoutSubscribersAcummulateMessages(t *testing.T) {
	q := NewQueue(NewQueueHandler)
	for i := 0; i < 100; i++ {
		q.Consume(testMsg{})
	}

	if q.Len() != 100 {
		t.Errorf("Queue has unexpected size %d != %d", q.Len(), 100)
	}
	q.Close()
}

func TestQueueCloseFlushesMessages(t *testing.T) {
	q := NewQueue(NewQueueHandler)
	c := new(countingConsumer)
	err := q.Subscribe(c)

	if err != nil {
		t.Error("Unexpected error")
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for i := 0; i < 100; i++ {
			q.Consume(testMsg{})
		}
		q.Close()
		wg.Done()
	}()

	wg.Wait()
	c.mu.RLock()
	if c.callsCount != 100 {
		t.Errorf("Queue did not flush on close, got: %d messages", c.callsCount)
	}
	c.mu.RUnlock()
}

func TestQueueBalancesMessagesBetweenConsumers(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	q := NewQueue(NewQueueHandler)

	c1 := new(countingConsumer)
	c2 := new(countingConsumer)
	c3 := new(countingConsumer)
	c4 := new(countingConsumer)

	q.Subscribe(c1)
	q.Subscribe(c2)
	q.Subscribe(c3)
	q.Subscribe(c4)

	go func() {
		for i := 0; i < 1000; i++ {
			q.Consume(testMsg{})
		}
		q.Close()
		wg.Done()
	}()

	wg.Wait()

	for _, consumer := range []*countingConsumer{c1, c2, c3, c4} {
		consumer.mu.RLock()
		if consumer.callsCount != 250 {
			t.Errorf("Consumer has unexpected call count %d expected %d", consumer.callsCount, 250)
		}
		consumer.mu.RUnlock()
	}
}

type testMsg struct {
	headers    Headers
	routingKey string
	priority   uint8
	body       []byte
}

func (self testMsg) Headers() Headers {
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

func (self *countingConsumer) Consume(msg Message) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.callsCount++
}
