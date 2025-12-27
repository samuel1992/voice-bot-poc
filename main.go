package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

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

func processInDeepSeek(deepseekClient *DeepSeekClient, messageText string) {
	finished, reply, err := deepseekClient.AnalyzeTranscript(messageText)
	if err != nil {
		log.Printf("[ERROR] DeepSeek error: %v", err)
		return
	}

	if finished && reply != "" {
		log.Printf("[DEEPSEEK] %s", reply)
	}
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

	elevenLabsClient := NewElevenLabsClient(elevenlabsAPIKey, elevenlabsVoiceID)
	defer elevenLabsClient.Close()

	fireworksClient := NewFireworksWSClient(
		"wss://audio-streaming.api.fireworks.ai/v1/audio/transcriptions/streaming",
		fireworksAPIKey,
	)

	if err := fireworksClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to Fireworks: %v", err)
	}
	defer fireworksClient.Stop()

	deepseekClient := NewDeepSeekClient(deepseekAPIKey, deepseekAPIURL, deepseekModel)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	conversation := NewConversation()

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
			case "callend":
				log.Println("Call ended")
			default:
				log.Printf("Unknown event: %s", event.Event)
			}

		case audioFrame := <-wsClient.binaryMsgs:
			if err := fireworksClient.SendAudio(audioFrame); err != nil {
				log.Printf("Error sending audio to Fireworks: %v", err)
			}

			if conversation.IsSilent() && conversation.currentMessage.IsNew() {
				go func() {
					finished, reply, err := deepseekClient.AnalyzeTranscript(conversation.currentMessage.text)
					if err != nil {
						log.Printf("[ERROR] DeepSeek error: %v", err)
						return
					}

					if finished && reply != "" {
						log.Printf("[DEEPSEEK] %s", reply)
						elevenLabsClient.StreamText(reply)
					}
				}()

				conversation.MarkCurrentMessageProcessed()
				log.Printf("[SILENCE-DETECTED] Processing '(%v) %s' with DeepSeek.\n",
					conversation.currentMessage.ID,
					conversation.currentMessage.text,
				)
			}

		case audioFrame := <-elevenLabsClient.GetAudioChannel():
			if err := wsClient.Conn.WriteMessage(websocket.BinaryMessage, audioFrame); err != nil {
				log.Printf("Error sending audio frame to websocket: %v", err)
			}

		case transcript := <-fireworksClient.TranscriptChan:
			conversation.Add(&transcript)
			if current := conversation.currentMessage; current != nil && current.IsNew() {
				log.Printf("[TRANSCRIPT] (%v) - %s", current.ID, current.text)
			}

		case <-wsClient.done:
			log.Println("WebSocket connection closed")
			return
		}
	}
}
