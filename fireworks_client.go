package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
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

// FireworksWSClient handles WebSocket streaming transcription
type FireworksWSClient struct {
	conn              *websocket.Conn
	apiKey            string
	wsURL             string
	language          string
	responseFormat    string
	currentTranscript string
	TranscriptChan    chan string
	done              chan struct{}
	audioFrameCount   int
	audioBuffer       []byte // Buffer to accumulate frames before sending
	bufferSize        int    // Target buffer size (5 frames = 1600 bytes = 100ms)
}

// NewFireworksWSClient creates a new WebSocket streaming transcription client
func NewFireworksWSClient(url, apiKey string) *FireworksWSClient {
	return &FireworksWSClient{
		wsURL:          url,
		apiKey:         apiKey,
		language:       "pt",
		responseFormat: "verbose_json",
		TranscriptChan: make(chan string, 10),
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
			fc.currentTranscript = response.Text
			select {
			case fc.TranscriptChan <- response.Text:
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
func (fc *FireworksWSClient) GetTranscriptChan() chan string {
	return fc.TranscriptChan
}

// SetLanguage sets the transcription language
func (fc *FireworksWSClient) SetLanguage(lang string) {
	fc.language = lang
}

// ===== FireworksAPIClient (REST API) =====

// FireworksAPIClient handles REST API transcription with VAD support
type FireworksAPIClient struct {
	httpClient        *http.Client
	apiKey            string
	apiURL            string
	language          string
	responseFormat    string
	vadModel          string
	currentTranscript string
	TranscriptChan    chan string
	done              chan struct{}
	audioBuffer       []byte // Auto-buffer frames before POSTing
	bufferSize        int    // Threshold size for POSTing (~2 seconds)
}

// NewFireworksAPIClient creates a new REST API transcription client
func NewFireworksAPIClient(url, apiKey string) *FireworksAPIClient {
	return &FireworksAPIClient{
		apiURL:         url,
		apiKey:         apiKey,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		language:       "pt",
		responseFormat: "verbose_json",
		vadModel:       "silero",
		TranscriptChan: make(chan string, 10),
		done:           make(chan struct{}),
		audioBuffer:    make([]byte, 0, 32000), // ~2 seconds at 16kHz
		bufferSize:     32000,                  // 2 seconds of audio for POST threshold
	}
}

// Connect is a no-op for API client (for interface compatibility)
func (fc *FireworksAPIClient) Connect() error {
	log.Println("Fireworks API client initialized")
	return nil
}

// SendAudio accumulates audio frames and POSTs when buffer reaches threshold
func (fc *FireworksAPIClient) SendAudio(audioFrame []byte) error {
	fc.audioBuffer = append(fc.audioBuffer, audioFrame...)

	if len(fc.audioBuffer) >= fc.bufferSize {
		audioData := make([]byte, len(fc.audioBuffer))
		copy(audioData, fc.audioBuffer)

		go func() {
			if err := fc.postTranscription(audioData); err != nil {
				log.Printf("Error posting transcription: %v", err)
			}
		}()

		fc.audioBuffer = fc.audioBuffer[:0]
	}

	return nil
}

// createWAVHeader creates a WAV file header for raw PCM audio
func createWAVHeader(dataSize int, sampleRate, numChannels, bitsPerSample int) []byte {
	header := make([]byte, 44)

	// RIFF header
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], uint32(36+dataSize))
	copy(header[8:12], "WAVE")

	// fmt subchunk
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16) // Subchunk1Size (16 for PCM)
	binary.LittleEndian.PutUint16(header[20:22], 1)  // AudioFormat (1 for PCM)
	binary.LittleEndian.PutUint16(header[22:24], uint16(numChannels))
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate))
	byteRate := sampleRate * numChannels * bitsPerSample / 8
	binary.LittleEndian.PutUint32(header[28:32], uint32(byteRate))
	blockAlign := numChannels * bitsPerSample / 8
	binary.LittleEndian.PutUint16(header[32:34], uint16(blockAlign))
	binary.LittleEndian.PutUint16(header[34:36], uint16(bitsPerSample))

	// data subchunk
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], uint32(dataSize))

	return header
}

// postTranscription sends audio data to Fireworks API via POST
func (fc *FireworksAPIClient) postTranscription(audioData []byte) error {
	// Create multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add audio file as multipart form data
	part, err := writer.CreateFormFile("file", "audio.wav")
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	// Create WAV file with proper headers
	// Assuming 8kHz, mono, 16-bit PCM (from WebSocket)
	wavHeader := createWAVHeader(len(audioData), 8000, 1, 16)
	part.Write(wavHeader)
	part.Write(audioData)

	// Add optional parameters
	if fc.language != "" {
		writer.WriteField("language", fc.language)
	}
	if fc.vadModel != "" {
		writer.WriteField("vad_model", fc.vadModel)
	}
	if fc.responseFormat != "" {
		writer.WriteField("response_format", fc.responseFormat)
	}

	writer.Close()

	// Create request
	req, err := http.NewRequest("POST", fc.apiURL, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+fc.apiKey)

	// Execute request
	resp, err := fc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to POST transcription: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(responseBody))
	}

	// Parse response
	var transcriptionResp TranscriptionResponse
	if err := json.Unmarshal(responseBody, &transcriptionResp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Send transcript to channel if available
	if transcriptionResp.Text != "" {
		fc.currentTranscript = transcriptionResp.Text
		select {
		case fc.TranscriptChan <- transcriptionResp.Text:
		default:
			log.Println("Transcript channel full, dropping update")
		}
	}

	return nil
}

// Stop flushes any remaining buffer and cleanup resources
func (fc *FireworksAPIClient) Stop() {
	log.Println("Stopping Fireworks API client...")

	// Flush remaining buffer if any
	if len(fc.audioBuffer) > 0 {
		log.Printf("Flushing remaining %d bytes of audio", len(fc.audioBuffer))
		audioData := make([]byte, len(fc.audioBuffer))
		copy(audioData, fc.audioBuffer)
		if err := fc.postTranscription(audioData); err != nil {
			log.Printf("Error flushing final audio: %v", err)
		}
		fc.audioBuffer = fc.audioBuffer[:0]
	}

	close(fc.done)
}

// GetTranscriptChan returns the transcript channel for receiving transcriptions
func (fc *FireworksAPIClient) GetTranscriptChan() chan string {
	return fc.TranscriptChan
}

// SetLanguage sets the transcription language
func (fc *FireworksAPIClient) SetLanguage(lang string) {
	fc.language = lang
}

// SetVADModel sets the VAD model to use
func (fc *FireworksAPIClient) SetVADModel(model string) {
	fc.vadModel = model
}
