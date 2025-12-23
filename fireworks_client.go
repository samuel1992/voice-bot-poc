package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

type TranscriptionResponse struct {
	Task      string `json:"task"`
	Language  string `json:"language"`
	Text      string `json:"text"`
	RequestID string `json:"request_id"`
	Words     []Word `json:"words"`
}

type Word struct {
	Word               string  `json:"word"`
	Language           string  `json:"language"`
	Probability        float64 `json:"probability"`
	HallucinationScore float64 `json:"hallucination_score"`
	Start              float64 `json:"start"`
	End                float64 `json:"end"`
	SpeakerID          string  `json:"speaker_id"`
	RetryCount         int     `json:"retry_count"`
}

type FireworksClient struct {
	conn              *websocket.Conn
	apiKey            string
	currentTranscript string
	TranscriptChan    chan string
	done              chan struct{}
	audioFrameCount   int
	audioBuffer       []byte // Buffer to accumulate frames before sending
	bufferSize        int    // Target buffer size (5 frames = 1600 bytes = 100ms)
}

func NewFireworksClient(apiKey string) *FireworksClient {
	return &FireworksClient{
		apiKey:         apiKey,
		TranscriptChan: make(chan string, 10),
		done:           make(chan struct{}),
		audioBuffer:    make([]byte, 0, 1600), // Pre-allocate for 5 frames
		bufferSize:     1600,                  // 5 frames × 320 bytes = 100ms
	}
}

func (fc *FireworksClient) Connect() error {
	baseURL := "wss://audio-streaming.api.fireworks.ai/v1/audio/transcriptions/streaming"
	params := url.Values{}
	params.Add("authorization", fc.apiKey)
	params.Add("language", "pt")
	params.Add("response_format", "verbose_json")

	fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

	log.Println("Connecting to Fireworks AI transcription service...")

	var err error
	fc.conn, _, err = websocket.DefaultDialer.Dial(fullURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Fireworks: %w", err)
	}

	log.Println("Fireworks WebSocket connected successfully")

	// Start listening for transcription messages
	go fc.listenForTranscriptions()

	return nil
}

func (fc *FireworksClient) listenForTranscriptions() {
	defer close(fc.done)

	for {
		_, message, err := fc.conn.ReadMessage()
		if err != nil {
			log.Printf("Fireworks read error: %v", err)
			return
		}

		var response TranscriptionResponse
		if err := json.Unmarshal(message, &response); err != nil {
			log.Printf("Failed to parse Fireworks transcription: %v", err)
			continue
		}

		if response.Text != "" {
			fc.currentTranscript = response.Text
			log.Printf("Transcription update: %s", response.Text)

			// Send to channel for main.go to process
			select {
			case fc.TranscriptChan <- response.Text:
			default:
				log.Println("Transcript channel full, dropping update")
			}
		}
	}
}

func (fc *FireworksClient) SendAudio(audioFrame []byte) error {
	if fc.conn == nil {
		return fmt.Errorf("not connected to Fireworks")
	}

	// Accumulate frames in buffer
	fc.audioBuffer = append(fc.audioBuffer, audioFrame...)

	// When buffer reaches target size, send it
	if len(fc.audioBuffer) >= fc.bufferSize {
		// Upsample from 8kHz to 16kHz (Fireworks requirement)
		upsampled := fc.upsampleBuffer(fc.audioBuffer)

		err := fc.conn.WriteMessage(websocket.BinaryMessage, upsampled)
		if err != nil {
			return fmt.Errorf("failed to send audio to Fireworks: %w", err)
		}

		fc.audioFrameCount++
		if fc.audioFrameCount%10 == 0 {
			log.Printf("Sent %d buffered chunks to Fireworks (%d bytes each)", fc.audioFrameCount, len(fc.audioBuffer))
		}

		// Clear buffer
		fc.audioBuffer = fc.audioBuffer[:0]
	}

	return nil
}

func (fc *FireworksClient) upsampleBuffer(audioData []byte) []byte {
	// Simple linear interpolation upsampling
	// Input: N bytes (N/2 samples @ 8kHz)
	// Output: N*2 bytes (N samples @ 16kHz)

	if len(audioData)%2 != 0 {
		log.Printf("Warning: audio data length not even: %d bytes", len(audioData))
		return audioData
	}

	numInputSamples := len(audioData) / 2

	// Convert bytes to int16 samples
	inputSamples := make([]int16, numInputSamples)
	for i := 0; i < numInputSamples; i++ {
		inputSamples[i] = int16(binary.LittleEndian.Uint16(audioData[i*2 : i*2+2]))
	}

	// Upsample: create 2 samples for each input sample
	outputSamples := make([]int16, numInputSamples*2)
	for i := 0; i < numInputSamples-1; i++ {
		outputSamples[i*2] = inputSamples[i]
		// Linear interpolation between current and next sample
		outputSamples[i*2+1] = int16((int32(inputSamples[i]) + int32(inputSamples[i+1])) / 2)
	}
	// Last two samples
	outputSamples[len(outputSamples)-2] = inputSamples[numInputSamples-1]
	outputSamples[len(outputSamples)-1] = inputSamples[numInputSamples-1]

	// Convert back to bytes
	outputBytes := make([]byte, numInputSamples*4)
	for i := 0; i < numInputSamples*2; i++ {
		binary.LittleEndian.PutUint16(outputBytes[i*2:i*2+2], uint16(outputSamples[i]))
	}

	return outputBytes
}

func (fc *FireworksClient) Stop() {
	if fc.conn != nil {
		log.Println("Closing Fireworks connection...")
		fc.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		fc.conn.Close()
	}
}
