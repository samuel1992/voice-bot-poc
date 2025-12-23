package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type DeepSeekRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type DeepSeekResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
}

type DeepSeekClient struct {
	apiKey       string
	apiURL       string
	model        string
	systemPrompt string
	httpClient   *http.Client
}

func NewDeepSeekClient(apiKey, apiURL, model string) *DeepSeekClient {
	systemPrompt := `You are a phone assistant for Vili Tecnologia. Vili Tecnologia provides a voip pbx and the price of the product
is 42 reais per extension and they require a minimum of 10 extensions per contract. They offer IVR, Automatic Dialers, Extensions, call reports, billing, realtime voice integrations.
Your job is to:
1. Analyze incoming transcription to determine if the caller has finished speaking
2. If still speaking: respond with exactly "LISTENING"
3. If finished: provide a helpful, natural Portuguese response

Context: You receive progressive transcription updates. The caller may pause mid-sentence.
Look for:
- Complete questions (ending with ?)
- Natural sentence endings
- Pauses after complete thoughts

Be conversational and friendly in Portuguese.`

	return &DeepSeekClient{
		apiKey:       apiKey,
		apiURL:       apiURL,
		model:        model,
		systemPrompt: systemPrompt,
		httpClient:   &http.Client{},
	}
}

func (dc *DeepSeekClient) AnalyzeTranscript(transcript string, chatHistory []ChatMessage) (bool, string, error) {
	// Build messages array: system + chat history + current transcript
	messages := []ChatMessage{
		{Role: "system", Content: dc.systemPrompt},
	}

	// Add chat history
	messages = append(messages, chatHistory...)

	// Add current transcript as user message
	messages = append(messages, ChatMessage{
		Role:    "user",
		Content: transcript,
	})

	// Create request
	reqBody := DeepSeekRequest{
		Model:    dc.model,
		Messages: messages,
		Stream:   false,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return false, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send POST request
	req, err := http.NewRequest("POST", dc.apiURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return false, "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+dc.apiKey)

	resp, err := dc.httpClient.Do(req)
	if err != nil {
		return false, "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, "", fmt.Errorf("DeepSeek API error: %d - %s", resp.StatusCode, string(body))
	}

	// Parse response
	var deepseekResp DeepSeekResponse
	if err := json.NewDecoder(resp.Body).Decode(&deepseekResp); err != nil {
		return false, "", fmt.Errorf("failed to decode response: %w", err)
	}

	if len(deepseekResp.Choices) == 0 {
		return false, "", fmt.Errorf("no choices in DeepSeek response")
	}

	responseText := strings.TrimSpace(deepseekResp.Choices[0].Message.Content)

	// Check if still listening
	if responseText == "LISTENING" {
		log.Println("[DEEPSEEK] Still listening...")
		return false, "", nil
	}

	// User is done, return the response
	log.Printf("[DEEPSEEK] User finished. Reply: %s", responseText)
	return true, responseText, nil
}
