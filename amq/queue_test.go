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

// Public interface tests
package amq_test

import (
	"sync"
	"testing"
	"time"

	"github.com/canni/paperboymq/amq"
	"github.com/canni/paperboymq/queue"
)

func TestMessageQueue_EmptyQueueHasZeroLength(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)

	if q.Len() != 0 {
		t.Errorf("Unexpected queue length")
	}
	q.Close()
}

func TestMessageQueue_ForceCloseOnEmptyQueue(t *testing.T) {
	// Just to get coverage, there is no way to distinguish close from force close on empty queue
	q := amq.NewQueue(queue.NewQueueHandler)

	if q.Len() != 0 {
		t.Errorf("Unexpected queue length")
	}
	q.ForceClose()
}

func TestMessageQueue_QueueWithoutSubscribersAcummulateMessages(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	for i := 0; i < 100; i++ {
		q.Consume(testMsg{})
	}

	if q.Len() != 100 {
		t.Errorf("Queue has unexpected size %d != %d", q.Len(), 100)
	}
	q.Close()
}

func TestMessageQueue_EmptySubscibersList(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	defer q.Close()

	list := q.Subscriptions()

	if len(list) != 0 {
		t.Error("Unexpected consumer in list")
	}

	if list != nil {
		t.Error("Unexpected type")
	}
}

func TestMessageQueue_SubscibersList(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	defer q.Close()

	subs := map[amq.MessageConsumer]struct{}{
		new(countingConsumer): struct{}{},
		new(countingConsumer): struct{}{},
		new(countingConsumer): struct{}{},
	}

	for c := range subs {
		err := q.Subscribe(c)
		if err != nil {
			t.Error("Unexpected error", err)
		}
	}

	list := q.Subscriptions()

	if len(list) != 3 {
		t.Error("Unexpected consumer count, expedted:", 4, "got:", len(list))
	}

	for _, c := range list {
		if _, found := subs[c]; !found {
			t.Error("Consumer not found in reference list")
		}
	}
}

func TestMessageQueue_SubscribeConsumer(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	defer q.Close()

	c := new(countingConsumer)

	err := q.Subscribe(c)
	if err != nil {
		t.Error("Unexpected error")
	}
}

func TestMessageQueue_SubscribeConsumerTwiceReturnsError(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	defer q.Close()

	c := new(countingConsumer)

	err := q.Subscribe(c)
	if err != nil {
		t.Error("Unexpected error")
	}

	err = q.Subscribe(c)
	if err != amq.ErrConsumerAlreadySubscribed {
		t.Error("Invalid error returned", err)
	}
}

func TestMessageQueue_UnsubscribeWithEmptySubscriptionsListReturnsError(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	defer q.Close()

	c := new(countingConsumer)

	err := q.Unsubscribe(c)
	if err != amq.ErrConsumerNotFound {
		t.Error("Invalid error returned", err)
	}
}

func TestMessageQueue_UnsubscribeTwiceReturnsError(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
	defer q.Close()

	c := new(countingConsumer)

	err := q.Subscribe(c)
	if err != nil {
		t.Error("Unexpected error")
	}

	err = q.Unsubscribe(c)
	if err != nil {
		t.Error("Unexpected error")
	}

	err = q.Unsubscribe(c)
	if err != amq.ErrConsumerNotFound {
		t.Error("Invalid error returned", err)
	}
}

func TestMessageQueue_QueueCloseFlushesMessages(t *testing.T) {
	q := amq.NewQueue(queue.NewQueueHandler)
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

func TestMessageQueue_QueueBalancesMessagesBetweenConsumers(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	q := amq.NewQueue(queue.NewQueueHandler)

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

type countingConsumer struct {
	mu         sync.RWMutex
	callsCount int
}

func (self *countingConsumer) Consume(msg amq.Message) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.callsCount++
}
