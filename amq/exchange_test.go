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

package amq_test

import (
	"testing"

	"github.com/canni/paperboymq/amq"
	"github.com/canni/paperboymq/matcher"
)

func TestExchange_SilentlyDropsMessagesWhenNoBindings(t *testing.T) {
	ex := amq.NewExchange(matcher.Direct)

	for i := 0; i < 100; i++ {
		ex.Consume(testMsg{})
	}
}

func TestExchange_BindToErrorsOnDoubleBind(t *testing.T) {
	ex := amq.NewExchange(matcher.Direct)

	b := amq.NewBinding("key", new(countingConsumer))

	err := ex.BindTo(b)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	err = ex.BindTo(b)
	if err != amq.ErrAlreadyBound {
		t.Error("Unexpected error:", err)
	}
}

func TestExchange_UnbindFromErrorsOnDoubleUnind(t *testing.T) {
	ex := amq.NewExchange(matcher.Direct)

	b := amq.NewBinding("key", new(countingConsumer))

	err := ex.BindTo(b)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	err = ex.UnbindFrom(b)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	err = ex.UnbindFrom(b)
	if err != amq.ErrBindingNotFound {
		t.Error("Unexpected error:", err)
	}
}

func TestExchange_UsesMatcherForMessagesDirectionToBindings(t *testing.T) {
	ex := amq.NewExchange(matcher.Direct)

	c1 := new(countingConsumer)
	c2 := new(countingConsumer)

	b1 := amq.NewBinding("key1", c1)
	b2 := amq.NewBinding("key2", c2)

	ex.BindTo(b1)
	ex.BindTo(b2)

	for i := 0; i < 100; i++ {
		ex.Consume(testMsg{
			routingKey: "key1",
		})
	}

	if c1.callsCount != 100 {
		t.Errorf("Unexpected calls count: %d, expected: %d", c1.callsCount, 100)
	}

	if c2.callsCount != 0 {
		t.Errorf("Unexpected calls count: %d, expected: %d", c1.callsCount, 0)
	}
}

func TestExchange_OnlyOneDeliveryToSingleConsumer(t *testing.T) {
	ex := amq.NewExchange(matcher.Direct)

	c := new(countingConsumer)

	b1 := amq.NewBinding("key", c)
	b2 := amq.NewBinding("key", c)

	ex.BindTo(b1)
	ex.BindTo(b2)

	for i := 0; i < 100; i++ {
		ex.Consume(testMsg{
			routingKey: "key",
		})
	}

	if c.callsCount != 100 {
		t.Errorf("Unexpected calls count: %d, expected: %d", c.callsCount, 100)
	}
}
