package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

// FireworksTranscriber is the common interface for both WebSocket and API clients
type FireworksTranscriber interface {
	Connect() error
	SendAudio(audioFrame []byte) error
	GetTranscriptChan() chan string
	Stop()
}

type TranscriptionResponse struct {
	Task      string    `json:"task"`
	Language  string    `json:"language"`
	Text      string    `json:"text"`
	RequestID string    `json:"request_id"`
	Segments  []Segment `json:"segments"`
	Words     []Word    `json:"words"`
}

type Segment struct {
	ID               int     `json:"id"`
	Seek             int     `json:"seek"`
	Text             string  `json:"text"`
	Language         string  `json:"language"`
	Tokens           []any   `json:"tokens"`
	NGeneratedTokens int     `json:"n_generated_tokens"`
	CompressionRatio float64 `json:"compression_ratio"`
	AvgLogprob       float64 `json:"avg_logprob"`
	RetryCount       int     `json:"retry_count"`
	Words            []Word  `json:"words"`
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

// FireworksWSClient handles WebSocket streaming transcription
type FireworksWSClient struct {
	conn            *websocket.Conn
	apiKey          string
	wsURL           string
	language        string
	responseFormat  string
	TranscriptChan  chan Message
	done            chan struct{}
	audioFrameCount int
	audioBuffer     []byte // Buffer to accumulate frames before sending
	bufferSize      int    // Target buffer size (5 frames = 1600 bytes = 100ms)
}

// NewFireworksWSClient creates a new WebSocket streaming transcription client
func NewFireworksWSClient(url, apiKey string) *FireworksWSClient {
	return &FireworksWSClient{
		wsURL:          url,
		apiKey:         apiKey,
		language:       "pt",
		responseFormat: "verbose_json",
		TranscriptChan: make(chan Message, 10),
		done:           make(chan struct{}),
		audioBuffer:    make([]byte, 0, 1600), // Pre-allocate for 5 frames
		bufferSize:     1600,                  // 5 frames × 320 bytes = 100ms
	}
}

func (fc *FireworksWSClient) Connect() error {
	params := url.Values{}
	params.Add("authorization", fc.apiKey)
	params.Add("language", fc.language)
	params.Add("response_format", fc.responseFormat)

	fullURL := fmt.Sprintf("%s?%s", fc.wsURL, params.Encode())

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

func (fc *FireworksWSClient) listenForTranscriptions() {
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
			lastSegment := response.Segments[len(response.Segments)-1]
			msg := Message{
				ID:        lastSegment.ID,
				text:      lastSegment.Text,
				timestamp: time.Now(),
				processed: false,
			}

			select {
			case fc.TranscriptChan <- msg:
			default:
				log.Println("Transcript channel full, dropping update")
			}
		}
	}
}

func (fc *FireworksWSClient) SendAudio(audioFrame []byte) error {
	if fc.conn == nil {
		return fmt.Errorf("not connected to Fireworks")
	}

	fc.audioBuffer = append(fc.audioBuffer, audioFrame...)

	if len(fc.audioBuffer) >= fc.bufferSize {
		upsampled := fc.upsampleBuffer(fc.audioBuffer)

		err := fc.conn.WriteMessage(websocket.BinaryMessage, upsampled)
		if err != nil {
			return fmt.Errorf("failed to send audio to Fireworks: %w", err)
		}

		fc.audioBuffer = fc.audioBuffer[:0]
	}

	return nil
}

func (fc *FireworksWSClient) upsampleBuffer(audioData []byte) []byte {
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

func (fc *FireworksWSClient) Stop() {
	if fc.conn != nil {
		log.Println("Closing Fireworks connection...")
		fc.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		fc.conn.Close()
	}
}

// GetTranscriptChan returns the transcript channel for receiving transcriptions
func (fc *FireworksWSClient) GetTranscriptChan() chan Message {
	return fc.TranscriptChan
}

// SetLanguage sets the transcription language
func (fc *FireworksWSClient) SetLanguage(lang string) {
	fc.language = lang
}
