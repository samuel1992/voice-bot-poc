package main

import (
	"strings"
	"time"
)

type Message struct {
	text      string
	timestamp time.Time
	processed bool
}

func (m *Message) Text() string {
	return m.text
}

type Conversation struct {
	delta          float64
	messages       []*Message
	currentMessage *Message
	processedText  string
}

func NewConversation() *Conversation {
	return &Conversation{
		messages:      []*Message{},
		delta:         1.0,
		processedText: "",
	}
}

func (c *Conversation) CurrentMessage() *Message {
	return c.currentMessage
}

func (c *Conversation) Add(text string) {
	text = strings.TrimPrefix(text, c.processedText)
	text = strings.TrimSpace(text)

	if c.currentMessage == nil {
		c.currentMessage = &Message{
			text:      text,
			timestamp: time.Now(),
			processed: false,
		}
	} else {
		c.currentMessage.text = text
		c.currentMessage.timestamp = time.Now()
	}
}

func (c *Conversation) MarkCurrentMessageProcessed() {
	if c.currentMessage != nil {
		c.currentMessage.processed = true
		c.messages = append(c.messages, c.currentMessage)
		c.processedText += " " + c.currentMessage.text
		c.processedText = strings.TrimSpace(c.processedText)
		c.currentMessage = nil
	}
}

func (c *Conversation) IsNew(text string) bool {
	// We should compare not using only a if but having a percentage of similary words, for example
	if c.currentMessage == nil {
		return true
	}
	return c.currentMessage.text != text && c.messages[len(c.messages)-1].text != text
}

func (c *Conversation) IsSilent() bool {
	if c.currentMessage == nil {
		return false
	}

	if c.currentMessage.processed {
		return false
	}

	if c.currentMessage.text == "" {
		return false
	}

	if time.Since(c.currentMessage.timestamp).Seconds() >= c.delta {
		return true
	}

	return false
}
