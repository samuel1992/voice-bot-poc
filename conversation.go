package main

import (
	"time"
)

type Message struct {
	ID        int
	text      string
	timestamp time.Time
	processed bool
}

func (m *Message) IsNew() bool {
	if m.text == "" {
		return false
	}

	if m.processed {
		return false
	}

	return true
}

type Conversation struct {
	delta          float64
	messages       []*Message
	currentMessage *Message
}

func NewConversation() *Conversation {
	return &Conversation{
		messages: []*Message{},
		delta:    1.0,
	}
}

func (c *Conversation) CurrentMessage() *Message {
	return c.currentMessage
}

func (c *Conversation) Add(msg *Message) {
	if c.currentMessage == nil {
		c.currentMessage = msg
		return
	}

	if c.currentMessage.ID == msg.ID {
		c.currentMessage.text = msg.text
		c.currentMessage.timestamp = msg.timestamp
		return
	}

	c.currentMessage = msg
	c.messages = append(c.messages, c.currentMessage)
	c.currentMessage = nil
}

func (c *Conversation) MarkCurrentMessageProcessed() {
	if c.currentMessage != nil {
		c.currentMessage.processed = true
	}
}

func (c *Conversation) IsSilent() bool {
	if c.currentMessage == nil {
		return false
	}

	if time.Since(c.currentMessage.timestamp).Seconds() >= c.delta {
		return true
	}

	return false
}
