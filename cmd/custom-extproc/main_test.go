package main

import (
	"encoding/json"
	"testing"

	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

func TestSemanticRouteModel(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"auto", "gpt-4o"},
		{"Auto", "gpt-4o"},
		{"auto-fast", "gpt-4o-mini"},
		{"auto-cheap", "gpt-4o-mini"},
		{"auto-quality", "gpt-4o"},
		{"gpt-4o", "gpt-4o"},
		{"unknown-model", "unknown-model"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := semanticRouteModel(tt.input)
			if got != tt.want {
				t.Errorf("semanticRouteModel(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRedactRequestFields(t *testing.T) {
	body := map[string]any{
		"model":       "gpt-4o",
		"ssn":         "123-45-6789",
		"credit_card": "4111111111111111",
		"messages":    []any{},
	}
	redactRequestFields(body)
	for _, key := range []string{"ssn", "credit_card"} {
		if body[key] != "[REDACTED]" {
			t.Errorf("expected %q to be [REDACTED], got %v", key, body[key])
		}
	}
	if body["model"] != "gpt-4o" {
		t.Errorf("model should not be redacted")
	}
}

func TestRedactResponseFields(t *testing.T) {
	body := map[string]any{
		"choices": []any{
			map[string]any{
				"message": map[string]any{
					"content": "Your SSN is 123-45-6789",
				},
			},
		},
	}
	redactResponseFields(body)
	choices := body["choices"].([]any)
	msg := choices[0].(map[string]any)["message"].(map[string]any)
	content := msg["content"].(string)
	if content != "Your [PII] is 123-45-6789" {
		t.Errorf("expected PII to be redacted, got %q", content)
	}
}

func TestProcessRequestBody_NonStreaming(t *testing.T) {
	body := map[string]any{
		"model":    "auto",
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	}
	bodyBytes, _ := json.Marshal(body)

	resp, isStreaming := processRequestBody(&extprocv3.ProcessingRequest_RequestBody{
		RequestBody: &extprocv3.HttpBody{Body: bodyBytes},
	})
	if isStreaming {
		t.Error("expected non-streaming")
	}
	if resp.ModeOverride != nil {
		t.Error("expected no mode override for non-streaming")
	}

	br := resp.GetRequestBody().GetResponse().GetBodyMutation().GetBody()
	var parsed map[string]any
	if err := json.Unmarshal(br, &parsed); err != nil {
		t.Fatalf("unmarshal response body: %v", err)
	}
	if parsed["model"] != "gpt-4o" {
		t.Errorf("expected model rewrite to gpt-4o, got %v", parsed["model"])
	}
}

func TestProcessRequestBody_Streaming(t *testing.T) {
	body := map[string]any{
		"model":    "gpt-4o",
		"stream":   true,
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	}
	bodyBytes, _ := json.Marshal(body)

	resp, isStreaming := processRequestBody(&extprocv3.ProcessingRequest_RequestBody{
		RequestBody: &extprocv3.HttpBody{Body: bodyBytes},
	})
	if !isStreaming {
		t.Error("expected streaming")
	}
	if resp.ModeOverride == nil {
		t.Fatal("expected mode override for streaming")
	}
	if resp.ModeOverride.ResponseBodyMode != extprocfilterv3.ProcessingMode_NONE {
		t.Errorf("expected response body NONE, got %v", resp.ModeOverride.ResponseBodyMode)
	}
	if resp.ModeOverride.ResponseHeaderMode != extprocfilterv3.ProcessingMode_SEND {
		t.Errorf("expected response header SEND, got %v", resp.ModeOverride.ResponseHeaderMode)
	}
}
