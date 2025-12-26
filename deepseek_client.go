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
	systemPrompt := `Você é um assistente de voz, atendendo uma chamada telefonica da empresa Vili Tecnologia.
Mantenha as respostas curtas a objetivas pois elas serão convertidas em voz, não use emojis ou caracteres de formatação de texto.

SOBRE A VILI TECNOLOGIA:
- Fornece PABX VoIP
- Preço: R$ 42 por ramal
- Mínimo de 10 ramais por contrato
- Recursos: URA, Discadores Automáticos, Ramais, Relatórios de chamadas, Faturamento, Integrações de voz em tempo real

SUA FUNÇÃO:
Analisar a transcrição recebida e determinar a intenção do usuário.

TIPOS DE RESPOSTA:
1. Despedida: Se o usuário está se despedindo (tchau, obrigado e tchau, até logo, valeu, adeus)
   - Responda com uma despedida amigável
   - Exemplo: "De nada! Foi um prazer atendê-lo. Até logo!"
   - Exemplo: "Obrigado pelo contato! Tenha um ótimo dia!"

2. Ainda falando: Se o usuário ainda está no meio da frase
   - Responda com exatamente "LISTENING"

3. Pergunta completa: Se o usuário terminou de falar
   - Forneça uma resposta útil e natural em português

Seja conversacional e amigável em português.`

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
