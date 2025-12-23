package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type ElevenLabsClient struct {
	conn        *websocket.Conn
	apiKey      string
	voiceID     string
	AudioFrames chan []byte // Output: paced frames for main loop
	frameQueue  chan []byte // Internal: unpaced frames from ElevenLabs
	done        chan struct{}
}

type TextInput struct {
	Text          string         `json:"text"`
	VoiceSettings *VoiceSettings `json:"voice_settings,omitempty"`
}

type VoiceSettings struct {
	Stability       float64 `json:"stability"`
	SimilarityBoost float64 `json:"similarity_boost"`
}

type AudioResponse struct {
	Audio   string `json:"audio"`   // base64 encoded
	IsFinal bool   `json:"isFinal"` // signals end of generation
}

func NewElevenLabsClient(apiKey, voiceID string) *ElevenLabsClient {
	return &ElevenLabsClient{
		apiKey:      apiKey,
		voiceID:     voiceID,
		AudioFrames: make(chan []byte, 100), // Output buffer
		frameQueue:  make(chan []byte, 500), // Large buffer for queuing
		done:        make(chan struct{}),
	}
}

func (elc *ElevenLabsClient) Connect() error {
	// Build WebSocket URL with query parameters
	baseURL := fmt.Sprintf("wss://api.elevenlabs.io/v1/text-to-speech/%s/stream-input", elc.voiceID)
	params := url.Values{}
	params.Add("model_id", "eleven_turbo_v2_5")
	params.Add("output_format", "pcm_8000") // 8kHz PCM to match virtual extension

	fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

	log.Println("Connecting to ElevenLabs TTS service...")

	// Create request headers with API key
	headers := http.Header{}
	headers.Add("xi-api-key", elc.apiKey)

	var err error
	elc.conn, _, err = websocket.DefaultDialer.Dial(fullURL, headers)
	if err != nil {
		return fmt.Errorf("failed to connect to ElevenLabs: %w", err)
	}

	log.Println("ElevenLabs WebSocket connected successfully")

	// Start listening for audio responses
	go elc.listenForAudio()

	// Start frame pacer (reads from queue, sends with timing)
	go elc.framePacer()

	return nil
}

func (elc *ElevenLabsClient) listenForAudio() {
	defer close(elc.done)
	defer close(elc.frameQueue) // Signal pacer to stop

	for {
		_, message, err := elc.conn.ReadMessage()
		if err != nil {
			log.Printf("ElevenLabs read error: %v", err)
			return
		}

		var response AudioResponse
		if err := json.Unmarshal(message, &response); err != nil {
			log.Printf("Failed to parse ElevenLabs audio response: %v", err)
			continue
		}

		if response.Audio != "" {
			// Decode base64 audio
			pcmData, err := base64.StdEncoding.DecodeString(response.Audio)
			if err != nil {
				log.Printf("Failed to decode base64 audio: %v", err)
				continue
			}

			// Chunk audio into 320-byte frames (20ms at 8kHz, 16-bit mono)
			frames := elc.chunkAudio(pcmData, 320)

			log.Printf("Received %d bytes of audio, queuing %d frames", len(pcmData), len(frames))

			// Queue frames sequentially (no goroutine!)
			for _, frame := range frames {
				select {
				case elc.frameQueue <- frame:
				default:
					log.Println("Frame queue full, dropping frame")
				}
			}
		}

		if response.IsFinal {
			log.Println("ElevenLabs finished generating audio")
		}
	}
}

func (elc *ElevenLabsClient) chunkAudio(pcmData []byte, frameSize int) [][]byte {
	var chunks [][]byte

	for i := 0; i < len(pcmData); i += frameSize {
		end := i + frameSize
		if end > len(pcmData) {
			// Pad last frame with zeros if needed
			chunk := make([]byte, frameSize)
			copy(chunk, pcmData[i:])
			chunks = append(chunks, chunk)
		} else {
			chunks = append(chunks, pcmData[i:end])
		}
	}

	return chunks
}

func (elc *ElevenLabsClient) framePacer() {
	for frame := range elc.frameQueue {
		// Send frame to main loop
		select {
		case elc.AudioFrames <- frame:
		default:
			log.Println("Audio output channel full, dropping frame")
		}

		// Pace at 20ms per frame (50 fps)
		time.Sleep(20 * time.Millisecond)
	}
	log.Println("Frame pacer stopped")
}

func (elc *ElevenLabsClient) SynthesizeText(text string) error {
	if elc.conn == nil {
		return fmt.Errorf("not connected to ElevenLabs")
	}

	// Add space at the end as recommended by ElevenLabs docs
	if text[len(text)-1] != ' ' {
		text = text + " "
	}

	textInput := TextInput{
		Text: text,
		VoiceSettings: &VoiceSettings{
			Stability:       0.5,
			SimilarityBoost: 0.75,
		},
	}

	jsonData, err := json.Marshal(textInput)
	if err != nil {
		return fmt.Errorf("failed to marshal text input: %w", err)
	}

	log.Printf("[ELEVENLABS] Synthesizing: %s", text)

	err = elc.conn.WriteMessage(websocket.TextMessage, jsonData)
	if err != nil {
		return fmt.Errorf("failed to send text to ElevenLabs: %w", err)
	}

	return nil
}

func (elc *ElevenLabsClient) Stop() {
	if elc.conn != nil {
		log.Println("Closing ElevenLabs connection...")

		// Send empty text to signal end
		emptyInput := TextInput{Text: ""}
		jsonData, _ := json.Marshal(emptyInput)
		elc.conn.WriteMessage(websocket.TextMessage, jsonData)

		elc.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		elc.conn.Close()
	}
}
