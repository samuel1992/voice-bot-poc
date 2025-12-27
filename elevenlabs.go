package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type ElevenLabsClient struct {
	apiKey          string
	voiceID         string
	conn            *websocket.Conn
	outputAudioChan chan []byte
	done            chan struct{}
}

type ElevenLabsResponse struct {
	Audio               string `json:"audio"`
	IsFinal             bool   `json:"isFinal"`
	NormalizedAlignment any    `json:"normalizedAlignment,omitempty"`
	Alignment           any    `json:"alignment,omitempty"`
}

func NewElevenLabsClient(apiKey, voiceID string) *ElevenLabsClient {
	return &ElevenLabsClient{
		apiKey:          apiKey,
		voiceID:         voiceID,
		outputAudioChan: make(chan []byte, 100),
		done:            make(chan struct{}),
	}
}

func (c *ElevenLabsClient) Connect() error {
	if c.conn != nil {
		c.conn.Close()
	}

	c.outputAudioChan = make(chan []byte, 100)
	c.done = make(chan struct{})

	url := fmt.Sprintf("wss://api.elevenlabs.io/v1/text-to-speech/%s/stream-input?output_format=pcm_8000&language_code=pt", c.voiceID)

	headers := http.Header{}
	headers.Add("xi-api-key", c.apiKey)

	log.Printf("Connecting to ElevenLabs: %s", url)

	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(url, headers)
	if err != nil {
		return fmt.Errorf("failed to connect to ElevenLabs: %w", err)
	}

	log.Println("Connected to ElevenLabs successfully")

	c.startOutputLoop()

	return nil
}

func (c *ElevenLabsClient) startOutputLoop() {
	go func() {
		defer close(c.outputAudioChan)

		var frameBuffer bytes.Buffer
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-c.done:
				// Client closing, send remaining frames and exit
				if frameBuffer.Len() > 0 {
					<-ticker.C
					frame := make([]byte, 320)
					frameBuffer.Read(frame)
					c.outputAudioChan <- frame
				}
				return

			default:
				var response ElevenLabsResponse
				if err := c.conn.ReadJSON(&response); err != nil {
					if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
						log.Println("ElevenLabs connection closed normally")
						// Send remaining frames before exit
						if frameBuffer.Len() > 0 {
							<-ticker.C
							frame := make([]byte, 320)
							frameBuffer.Read(frame)
							c.outputAudioChan <- frame
						}
						return
					}
					log.Printf("Error reading from ElevenLabs: %v", err)
					return
				}

				// Decode and buffer audio
				if response.Audio != "" {
					audioData, err := base64.StdEncoding.DecodeString(response.Audio)
					if err != nil {
						log.Printf("Failed to decode audio data: %v", err)
						continue
					}

					log.Printf("Received audio chunk from ElevenLabs, size: %d", len(audioData))

					frameBuffer.Write(audioData)
					for frameBuffer.Len() >= 320 {
						<-ticker.C // Wait for 20ms timing

						frame := make([]byte, 320)
						frameBuffer.Read(frame)
						c.outputAudioChan <- frame
					}
				}

				if response.IsFinal {
					log.Println("Received final audio chunk from ElevenLabs")
					// Send any remaining partial frame (padded)
					if frameBuffer.Len() > 0 {
						<-ticker.C
						frame := make([]byte, 320)
						frameBuffer.Read(frame)
						c.outputAudioChan <- frame
					}
					// Clear the frame buffer for next stream
					frameBuffer.Reset()
				}
			}
		}
	}()
}

func chunkBySentence(text string) []string {
	// Pattern matches sentences ending with . ! or ?
	sentencePattern := regexp.MustCompile(`[^.!?]+[.!?]+`)
	sentences := sentencePattern.FindAllString(text, -1)
	var chunks []string
	for _, sentence := range sentences {
		trimmed := strings.TrimSpace(sentence)
		if trimmed != "" {
			chunks = append(chunks, trimmed)
		}
	}

	return chunks
}

func (c *ElevenLabsClient) StreamText(text string) error {
	// Reconnect before each stream
	if err := c.Connect(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	initMsg := map[string]any{
		"text": " ",
	}
	if err := c.conn.WriteJSON(initMsg); err != nil {
		return fmt.Errorf("failed to send init message: %w", err)
	}
	log.Println("Sent initialization message to ElevenLabs")

	chunks := chunkBySentence(text)
	if len(chunks) == 0 {
		chunks = []string{text}
	}

	for i, chunk := range chunks {
		textMsg := map[string]any{
			"text":                   chunk + " ",
			"try_trigger_generation": true,
		}
		if err := c.conn.WriteJSON(textMsg); err != nil {
			return fmt.Errorf("failed to send text chunk %d: %w", i+1, err)
		}
		log.Printf("Sent chunk %d/%d: %s", i+1, len(chunks), chunk)
	}

	closeMsg := map[string]any{
		"text": "",
	}
	if err := c.conn.WriteJSON(closeMsg); err != nil {
		return fmt.Errorf("failed to send close message: %w", err)
	}
	log.Println("Sent close message to ElevenLabs")

	return nil
}

func (c *ElevenLabsClient) GetAudioChannel() <-chan []byte {
	return c.outputAudioChan
}

func (c *ElevenLabsClient) Close() {
	if c.conn != nil {
		// Signal the output loop to exit
		close(c.done)

		c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.conn.Close()
		log.Println("Closed ElevenLabs connection")
	}
}
