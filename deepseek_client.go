package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
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
	systemPrompt := `Você é um assistente de voz atendendo uma chamada telefonica para a empresa Vili Tecnologia.
SUA FUNÇÃO É  ANALISAR O TEXTO RECEBIDO E RESPONDER AO USUÁRIO COM AS INFORMAÇÕES PERTINENTES A VILI TECNOLOGIA.
SEJA CONVERSACIONAL E AMIGAGEL EM PORTUGUES.
MANTENHA AS RESPOSTAS CURTAS A OBJETIVAS POIS ELAS SERÃO CONVERTIDAS EM VOZ, NÃO USE EMOJIS OU CARACTERES DE FORMATAÇÃO DE TEXTO. 
FORNEÇA UMA RESPOSTA ÚTIL E NATURAL EM PORTUGUÊS.

SOBRE A VILI TECNOLOGIA:
- Fornece PABX VoIP
- Preço: 42 reais por ramal
- Mínimo de 10 ramais por contrato
- Recursos: URA, Discadores Automáticos, Ramais, Relatórios de chamadas, Billing, Bots de voz, Integrações de voz em realtime.
`

	return &DeepSeekClient{
		apiKey:       apiKey,
		apiURL:       apiURL,
		model:        model,
		systemPrompt: systemPrompt,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (dc *DeepSeekClient) AnalyzeTranscript(transcript string) (bool, string, error) {
	messages := []ChatMessage{
		{Role: "system", Content: dc.systemPrompt},
		{Role: "user", Content: transcript},
	}

	reqBody := DeepSeekRequest{
		Model:    dc.model,
		Messages: messages,
		Stream:   false,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return false, "", fmt.Errorf("failed to marshal request: %w", err)
	}

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

	var deepseekResp DeepSeekResponse
	if err := json.NewDecoder(resp.Body).Decode(&deepseekResp); err != nil {
		return false, "", fmt.Errorf("failed to decode response: %w", err)
	}

	if len(deepseekResp.Choices) == 0 {
		return false, "", fmt.Errorf("no choices in DeepSeek response")
	}

	responseText := strings.TrimSpace(deepseekResp.Choices[0].Message.Content)

	if responseText == "LISTENING" {
		log.Println("[DEEPSEEK] Still listening...")
		return false, "", nil
	}

	return true, responseText, nil
}
