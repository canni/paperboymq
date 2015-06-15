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

// QueueHandler is an interface used by Queue implementation for internal
// queueing of messages, implementors does not need to worry about concurrent
// access.
type QueueHandler interface {
	Add(Message)
	Peek() Message
	Remove()
	Len() int
}

// Queue is a base AMQ entity, it supports goroutine-safe concurrent access.
//
// Queue needs to be initialized by calling NewQueue()
//
// Queue delivers messages to subscribed MessageConsumers in a round-robin
// fashion. Queue MUST be closed after use, either by calling Close()
// witch will flush all held messages to subscibed consumers (if any),
// or by calling ForceClose(), witch will drop messages and exit immediately.
type Queue struct {
	input, output chan Message
	subscribeOp   chan *subscriptionOp
	subscriptions chan []MessageConsumer
	lenght        chan int
	quit, quitCnf chan bool
	queueFactory  func() QueueHandler
}

// NewQueue returns initialized Queue.
func NewQueue(handlerFactory func() QueueHandler) *Queue {
	q := &Queue{
		input:         make(chan Message),
		output:        make(chan Message),
		subscribeOp:   make(chan *subscriptionOp),
		subscriptions: make(chan []MessageConsumer),
		lenght:        make(chan int),
		quit:          make(chan bool),
		quitCnf:       make(chan bool),
		queueFactory:  handlerFactory,
	}

	go q.inputHandler()
	go q.outputHandler()

	return q
}

// Consume enqueues message in Queue, it's safe to call this method from
// multiple goroutines.
func (self *Queue) Consume(msg Message) {
	self.input <- msg
}

// Subscribe new consumer in a round-robin ring, it's safe to call this method
// from multiple goroutines.
//
// If consumer is already subscribed the returned error will be of type:
// ErrConsumerAlreadySubscribed
func (self *Queue) Subscribe(consumer MessageConsumer) error {
	result := make(chan error)
	self.subscribeOp <- &subscriptionOp{
		subscribe: true,
		consumer:  consumer,
		result:    result,
	}

	return <-result
}

// Unsubscribe consumer from round-robin ring, it's safe to call this method
// from multiple goroutines.
//
// If consumer isn't already subscribed the returned error will be of type:
// ErrConsumerNotFound
func (self *Queue) Unsubscribe(consumer MessageConsumer) error {
	result := make(chan error)
	self.subscribeOp <- &subscriptionOp{
		subscribe: false,
		consumer:  consumer,
		result:    result,
	}

	return <-result
}

// Subscriptions return list of currently subscribed consumers, it's safe
// to call this method from multiple goroutines.
func (self *Queue) Subscriptions() []MessageConsumer {
	self.subscriptions <- nil
	return <-self.subscriptions
}

// Len returns count of currently held messages in Queue, it's safe to call this
// method from multiple goroutines.
func (self *Queue) Len() int {
	return <-self.lenght
}

// Close gracefully flushes messages to all subscribed consumers (if any),
// and closes Queue.
//
// Is an error to use queue after it has been closed.
func (self *Queue) Close() {
	self.close(false)
}

// Close drops all messages and closes Queue.
//
// Is an error to use queue after it has been force-closed.
func (self *Queue) ForceClose() {
	self.close(true)
}

func (self *Queue) close(force bool) {
	self.quit <- force
	self.quit <- force
	<-self.quitCnf
	<-self.quitCnf
}

func (self *Queue) inputHandler() {
	q := self.queueFactory()

	for {
		if q.Len() > 0 {
			select {
			case msg := <-self.input:
				q.Add(msg)

			case self.output <- q.Peek():
				q.Remove()

			case self.lenght <- q.Len():
				// Nothing here

			case force := <-self.quit:
				if !force {
					for q.Len() > 0 {
						self.output <- q.Peek()
						q.Remove()
					}
					close(self.output)
				}
				self.quitCnf <- true
				return
			}
		} else {
			select {
			case msg := <-self.input:
				q.Add(msg)

			case self.lenght <- 0:
				// Nothing here

			case force := <-self.quit:
				if !force {
					close(self.output)
				}
				self.quitCnf <- true
				return
			}
		}
	}
}

func (self *Queue) outputHandler() {
	rr := newRoundRobinHandler()

	for {
		if rr.Len() > 0 {
			select {
			case msg := <-self.output:
				rr.Next().Consume(msg)

			case op := <-self.subscribeOp:
				if op.subscribe {
					op.result <- rr.Add(op.consumer)
				} else {
					op.result <- rr.Remove(op.consumer)
				}

			case <-self.subscriptions:
				list := make([]MessageConsumer, 0, rr.Len())
				for subscriber := range rr.consumers {
					list = append(list, subscriber)
				}
				self.subscriptions <- list

			case force := <-self.quit:
				if !force {
					for msg := range self.output {
						rr.Next().Consume(msg)
					}
				}
				self.quitCnf <- true
				return
			}
		} else {
			select {
			case op := <-self.subscribeOp:
				if op.subscribe {
					op.result <- rr.Add(op.consumer)
				} else {
					op.result <- ErrConsumerNotFound
				}

			case <-self.subscriptions:
				self.subscriptions <- nil

			case force := <-self.quit:
				if !force {
					for _ = range self.output {
					}
				}
				self.quitCnf <- true
				return
			}
		}
	}
}

type subscriptionOp struct {
	subscribe bool
	consumer  MessageConsumer
	result    chan error
}

// Ensure *Queue implements Consumer interface
var _ MessageConsumer = &Queue{}

// Ensure *Queue implements RR Dispatch inetrface
var _ RoundRobinDispatcher = &Queue{}
