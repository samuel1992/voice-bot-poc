package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Event struct {
	Event    string                 `json:"event"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	// Load configuration
	host := getEnv("EXTENSION_HOST", "localhost")
	port := getEnv("EXTENSION_PORT", "8383")
	companyID := getEnv("COMPANY_ID", "vili01")
	extension := getEnv("EXTENSION", "3001")
	fireworksAPIKey := getEnv("FIREWORKS_API_KEY", "")
	deepseekAPIKey := getEnv("DEEPSEEK_API_KEY", "")
	deepseekAPIURL := getEnv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")
	deepseekModel := getEnv("DEEPSEEK_MODEL", "deepseek-chat")
	elevenlabsAPIKey := getEnv("ELEVENLABS_API_KEY", "")
	elevenlabsVoiceID := getEnv("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")

	if fireworksAPIKey == "" {
		log.Fatal("FIREWORKS_API_KEY environment variable is required")
	}
	if deepseekAPIKey == "" {
		log.Fatal("DEEPSEEK_API_KEY environment variable is required")
	}
	if elevenlabsAPIKey == "" {
		log.Fatal("ELEVENLABS_API_KEY environment variable is required")
	}

	// Initialize clients
	wsClient := NewWsClient(host, port, companyID, extension)
	wsClient.Connect()
	wsClient.Start()

	fireworksClient := NewFireworksClient(fireworksAPIKey)
	if err := fireworksClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to Fireworks: %v", err)
	}
	defer fireworksClient.Stop()

	deepseekClient := NewDeepSeekClient(deepseekAPIKey, deepseekAPIURL, deepseekModel)

	elevenlabsClient := NewElevenLabsClient(elevenlabsAPIKey, elevenlabsVoiceID)
	if err := elevenlabsClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to ElevenLabs: %v", err)
	}
	defer elevenlabsClient.Stop()

	// Chat history for maintaining conversation context
	var chatHistory []ChatMessage

	// Debouncing variables for DeepSeek processing
	var (
		lastTranscript  string // Latest transcript from Fireworks
		lastProcessed   string // Last transcript we sent to DeepSeek
		transcriptTimer *time.Timer
		processingMutex sync.Mutex
		isProcessing    bool // Prevent concurrent DeepSeek processing
	)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	for {
		select {
		case <-interrupt:
			log.Println("Interrupt signal received, closing connection...")
			wsClient.Stop()
			return

		case textMsg := <-wsClient.textMsgs:
			var event Event
			if err := json.Unmarshal([]byte(textMsg), &event); err != nil {
				log.Printf("Failed to parse JSON: %v", err)
				continue
			}

			switch event.Event {
			case "callstart":
				metadataJSON, _ := json.Marshal(event.Metadata)
				log.Printf("Call started - metadata: %s", metadataJSON)

				// Greet the caller
				greeting := "Olá! Bem-vindo à Vili Tecnologia. Como posso ajudá-lo?"
				log.Printf("[GREETING] %s", greeting)

				// Add to chat history
				processingMutex.Lock()
				chatHistory = append(chatHistory, ChatMessage{
					Role:    "assistant",
					Content: greeting,
				})
				processingMutex.Unlock()

				// Send greeting via TTS
				if err := elevenlabsClient.SynthesizeText(greeting); err != nil {
					log.Printf("Failed to synthesize greeting: %v", err)
				}
			case "callend":
				log.Println("Call ended")
			default:
				log.Printf("Unknown event: %s", event.Event)
			}

		case audioFrame := <-wsClient.binaryMsgs:
			// Forward audio to Fireworks for transcription
			if err := fireworksClient.SendAudio(audioFrame); err != nil {
				log.Printf("Error sending audio to Fireworks: %v", err)
			}

		case transcript := <-fireworksClient.TranscriptChan:
			log.Printf("[TRANSCRIPT] %s", transcript)

			// Debounce: only process after 500ms of no updates
			processingMutex.Lock()
			lastTranscript = transcript

			// Reset timer - wait for 500ms pause before processing
			if transcriptTimer != nil {
				transcriptTimer.Stop()
			}

			transcriptTimer = time.AfterFunc(500*time.Millisecond, func() {
				processingMutex.Lock()

				// Check if already processing
				if isProcessing {
					processingMutex.Unlock()
					log.Println("[DEEPSEEK] Already processing, skipping...")
					return
				}

				// Capture current transcript
				currentTranscript := lastTranscript

				// Check if we already processed this exact text
				if currentTranscript == lastProcessed {
					processingMutex.Unlock()
					log.Println("[DEEPSEEK] Already processed this transcript, skipping...")
					return
				}

				// Set processing flag
				isProcessing = true
				processingMutex.Unlock()

				log.Println("[DEEPSEEK] Processing new transcript...")

				// Send to DeepSeek for turn detection (this can take 1-2 seconds)
				shouldReply, response, err := deepseekClient.AnalyzeTranscript(currentTranscript, chatHistory)
				if err != nil {
					log.Printf("DeepSeek error: %v", err)
					processingMutex.Lock()
					isProcessing = false
					processingMutex.Unlock()
					return
				}

				if shouldReply {
					log.Printf("[RESPONSE] %s", response)

					// Update chat history, mark as processed, and clear processing flag
					processingMutex.Lock()
					chatHistory = append(chatHistory,
						ChatMessage{Role: "user", Content: currentTranscript},
						ChatMessage{Role: "assistant", Content: response},
					)
					lastProcessed = currentTranscript
					isProcessing = false
					processingMutex.Unlock()

					// Synthesize response to audio
					if err := elevenlabsClient.SynthesizeText(response); err != nil {
						log.Printf("Failed to synthesize response: %v", err)
					}
				} else {
					// Still listening, mark as processed and clear processing flag
					processingMutex.Lock()
					lastProcessed = currentTranscript
					isProcessing = false
					processingMutex.Unlock()
				}
			})

			processingMutex.Unlock()

		case audioFrame := <-elevenlabsClient.AudioFrames:
			// Send audio back to caller via virtual extension WebSocket
			if err := wsClient.Conn.WriteMessage(websocket.BinaryMessage, audioFrame); err != nil {
				log.Printf("Error sending audio to caller: %v", err)
			}

		case <-wsClient.done:
			log.Println("WebSocket connection closed")
			return
		}
	}
}
