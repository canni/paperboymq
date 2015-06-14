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

type QueueHandler interface {
	Add(Message)
	Peek() Message
	Remove()
	Len() int
}

type Queue struct {
	input, output chan Message
	subscriptions chan *subscriptionOp
	lenght        chan int
	quit, quitCnf chan bool
	queueFactory  func() QueueHandler
}

func NewQueue(handlerFactory func() QueueHandler) *Queue {
	q := &Queue{
		input:         make(chan Message),
		output:        make(chan Message),
		subscriptions: make(chan *subscriptionOp),
		lenght:        make(chan int),
		quit:          make(chan bool),
		quitCnf:       make(chan bool),
		queueFactory:  handlerFactory,
	}

	go q.inputHandler()
	go q.outputHandler()

	return q
}

func (self *Queue) Consume(msg Message) {
	self.input <- msg
}

func (self *Queue) Subscribe(consumer MessageConsumer) error {
	result := make(chan error)
	self.subscriptions <- &subscriptionOp{
		subscribe: true,
		consumer:  consumer,
		result:    result,
	}

	return <-result
}

func (self *Queue) Unsubscbe(consumer MessageConsumer) error {
	result := make(chan error)
	self.subscriptions <- &subscriptionOp{
		subscribe: false,
		consumer:  consumer,
		result:    result,
	}

	return <-result
}

func (self *Queue) Len() int {
	return <-self.lenght
}

func (self *Queue) Close() {
	self.close(false)
}

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

			case op := <-self.subscriptions:
				if op.subscribe {
					op.result <- rr.Add(op.consumer)
				} else {
					op.result <- rr.Remove(op.consumer)
				}

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
			case op := <-self.subscriptions:
				if op.subscribe {
					op.result <- rr.Add(op.consumer)
				} else {
					op.result <- ErrConsumerNotFound
				}

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
