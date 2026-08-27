// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// custom-extproc is a dedicated Envoy ext_proc gRPC server (port 9000)
// that performs semantic routing (model rewrite) and guardrail redaction
// on AI Gateway routes, opt-in via the Composite filter header match.
//
// Request flow:
//   - Request headers+buffered body: optionally redact sensitive fields,
//     rewrite the JSON "model" field to a different model name.
//   - If the body has "stream": true, set a mode override so the response
//     body is NOT buffered (streaming responses pass through).
//   - Otherwise: response headers + buffered response body redaction.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func main() {
	addr := ":9000"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down custom ext_proc")
		cancel()
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	srv := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(srv, &processor{})
	grpc_health_v1.RegisterHealthServer(srv, &healthServer{})

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Printf("custom ext_proc listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// healthServer implements the gRPC health check protocol.
type healthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (h *healthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// processor implements envoy.service.ext_proc.v3.ExternalProcessor.
type processor struct {
	extprocv3.UnimplementedExternalProcessorServer
}

func (p *processor) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv: %v", err)
		}

		var resp *extprocv3.ProcessingResponse
		switch v := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			resp = processRequestHeaders(v)
		case *extprocv3.ProcessingRequest_RequestBody:
			resp, _ = processRequestBody(v)
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			resp = processResponseHeaders(v)
		case *extprocv3.ProcessingRequest_ResponseBody:
			resp = processResponseBody(v)
		default:
			resp = &extprocv3.ProcessingResponse{
				Response: &extprocv3.ProcessingResponse_RequestHeaders{
					RequestHeaders: &extprocv3.HeadersResponse{},
				},
			}
		}
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "send: %v", err)
		}
	}
}

func processRequestHeaders(_ *extprocv3.ProcessingRequest_RequestHeaders) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{
					Status: extprocv3.CommonResponse_CONTINUE,
				},
			},
		},
	}
}

// processRequestBody parses the JSON body, optionally rewrites the "model" field
// (semantic routing) and redacts sensitive input fields. It also detects streaming
// mode and returns a mode override to skip buffering the response body.
func processRequestBody(v *extprocv3.ProcessingRequest_RequestBody) (*extprocv3.ProcessingResponse, bool) {
	body := v.RequestBody.GetBody()
	var parsed map[string]any
	isStreaming := false

	if err := json.Unmarshal(body, &parsed); err == nil {
		if s, ok := parsed["stream"].(bool); ok && s {
			isStreaming = true
		}

		// Semantic routing: rewrite the model field.
		// In a production system this would consult an embedding index or routing
		// table. Here we apply a simple prefix-based rule as a placeholder.
		if model, ok := parsed["model"].(string); ok {
			if rewritten := semanticRouteModel(model); rewritten != model {
				parsed["model"] = rewritten
				log.Printf("semantic route: %q -> %q", model, rewritten)
			}
		}

		// Guardrail: redact sensitive fields from the request.
		redactRequestFields(parsed)

		if newBody, err := json.Marshal(parsed); err == nil {
			body = newBody
		}
	}

	resp := &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{
				Response: &extprocv3.CommonResponse{
					Status: extprocv3.CommonResponse_CONTINUE,
					BodyMutation: &extprocv3.BodyMutation{
						Mutation: &extprocv3.BodyMutation_Body{Body: body},
					},
					HeaderMutation: &extprocv3.HeaderMutation{
						SetHeaders: []*corev3.HeaderValueOption{{
							Header: &corev3.HeaderValue{
								Key:      "content-length",
								RawValue: []byte(fmt.Sprintf("%d", len(body))),
							},
						}},
					},
				},
			},
		},
	}

	// When streaming, tell Envoy to skip response body processing via mode
	// override, since we don't want to buffer SSE chunks.
	if isStreaming {
		resp.ModeOverride = &extprocfilterv3.ProcessingMode{
			ResponseHeaderMode: extprocfilterv3.ProcessingMode_SEND,
			ResponseBodyMode:   extprocfilterv3.ProcessingMode_NONE,
		}
	}

	return resp, isStreaming
}

func processResponseHeaders(_ *extprocv3.ProcessingRequest_ResponseHeaders) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{
					Status: extprocv3.CommonResponse_CONTINUE,
				},
			},
		},
	}
}

// processResponseBody applies guardrail redaction to non-streaming response bodies.
func processResponseBody(v *extprocv3.ProcessingRequest_ResponseBody) *extprocv3.ProcessingResponse {
	body := v.ResponseBody.GetBody()
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err == nil {
		redactResponseFields(parsed)
		if newBody, err := json.Marshal(parsed); err == nil {
			body = newBody
		}
	}

	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ResponseBody{
			ResponseBody: &extprocv3.BodyResponse{
				Response: &extprocv3.CommonResponse{
					Status: extprocv3.CommonResponse_CONTINUE,
					BodyMutation: &extprocv3.BodyMutation{
						Mutation: &extprocv3.BodyMutation_Body{Body: body},
					},
					HeaderMutation: &extprocv3.HeaderMutation{
						SetHeaders: []*corev3.HeaderValueOption{{
							Header: &corev3.HeaderValue{
								Key:      "content-length",
								RawValue: []byte(fmt.Sprintf("%d", len(body))),
							},
						}},
					},
				},
			},
		},
	}
}

// semanticRouteModel applies placeholder semantic routing logic.
// In production, this would look up an embedding store or routing
// configuration to pick the best model for the prompt.
func semanticRouteModel(model string) string {
	routingTable := map[string]string{
		"auto":         "gpt-4o",
		"auto-fast":    "gpt-4o-mini",
		"auto-cheap":   "gpt-4o-mini",
		"auto-quality": "gpt-4o",
	}
	if target, ok := routingTable[strings.ToLower(model)]; ok {
		return target
	}
	return model
}

// redactRequestFields removes or masks sensitive fields from request bodies.
func redactRequestFields(body map[string]any) {
	for _, key := range []string{"ssn", "credit_card", "password", "secret"} {
		if _, ok := body[key]; ok {
			body[key] = "[REDACTED]"
		}
	}
}

// redactResponseFields removes or masks sensitive fields from response bodies.
func redactResponseFields(body map[string]any) {
	if choices, ok := body["choices"].([]any); ok {
		for _, choice := range choices {
			if m, ok := choice.(map[string]any); ok {
				if msg, ok := m["message"].(map[string]any); ok {
					if content, ok := msg["content"].(string); ok {
						msg["content"] = redactPII(content)
					}
				}
			}
		}
	}
}

// redactPII is a placeholder PII redaction function.
// In production this would call a dedicated guardrail service or use regex patterns.
func redactPII(content string) string {
	for _, pattern := range []string{"SSN", "ssn"} {
		content = strings.ReplaceAll(content, pattern, "[PII]")
	}
	return content
}
