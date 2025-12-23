package main

import (
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

type WsClient struct {
	host      string
	port      string
	companyID string
	extension string
	Conn      *websocket.Conn
	done      chan struct{}

	textMsgs   chan string
	binaryMsgs chan []byte
}

func NewWsClient(host, port, companyID, extension string) *WsClient {
	return &WsClient{
		host:       host,
		port:       port,
		companyID:  companyID,
		extension:  extension,
		done:       make(chan struct{}),
		textMsgs:   make(chan string, 10),
		binaryMsgs: make(chan []byte, 100),
	}
}

func (ws *WsClient) Connect() {
	url := fmt.Sprintf("ws://%s:%s/ws/virtual-extension/%s/%s", ws.host, ws.port, ws.companyID, ws.extension)
	log.Printf("Connecting to %s...", url)

	var err error
	ws.Conn, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	log.Println("WebSocket connected successfully")
}

func (ws *WsClient) Stop() {
	if ws.Conn != nil {
		err := ws.Conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Printf("Error sending close message: %v", err)
		}
		ws.Conn.Close()
	}
}

func (ws *WsClient) Start() {
	go func() {
		defer close(ws.done)

		for {
			messageType, message, err := ws.Conn.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v", err)
				return
			}

			switch messageType {
			case websocket.TextMessage:
				select {
				case ws.textMsgs <- string(message):
				default:
					log.Println("Text message channel is full, dropping message")

				}
			case websocket.BinaryMessage:
				select {
				case ws.binaryMsgs <- message:
				default:
					log.Println("Binary message channel is full, dropping message")
				}
			}
		}
	}()
}
