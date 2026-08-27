// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// custom-ext-server is an Envoy Gateway extension server that runs after
// AI Gateway. It injects a Composite HTTP filter (with header opt-in) that
// wraps a custom ext_proc, positioned immediately before the AI Gateway
// ext_proc filter in the HCM chain. This lets the custom ext_proc do
// semantic routing and guardrail processing on existing AI Gateway routes
// without adding new xDS routes or clusters for backends.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	xdscorev3 "github.com/cncf/xds/go/xds/core/v3"
	xdsmatcherv3 "github.com/cncf/xds/go/xds/type/matcher/v3"
	egextension "github.com/envoyproxy/gateway/proto/extension"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	compositev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/composite/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	httpconnectionmanagerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	commonmatchingv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/common/matching/v3"
	matcherv3 "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	customExtProcClusterName  = "custom-extproc"
	compositeFilterName       = "envoy.filters.http.composite/custom-extproc"
	nestedExtProcName         = "envoy.filters.http.ext_proc/custom"
	aiGatewayExtProcName      = "envoy.filters.http.ext_proc/aigateway"
	optInHeaderName           = "x-enable-custom-extproc"
	optInHeaderValue          = "true"
	defaultExtProcServiceFQDN = "custom-extproc.envoy-ai-gateway-system.svc.cluster.local"
	defaultExtProcServicePort = 9000
	defaultListenAddr         = ":1063"
)

func main() {
	addr := defaultListenAddr
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}
	serviceFQDN := defaultExtProcServiceFQDN
	if v := os.Getenv("EXTPROC_SERVICE_FQDN"); v != "" {
		serviceFQDN = v
	}
	servicePort := uint32(defaultExtProcServicePort)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down custom extension server")
		cancel()
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	srv := grpc.NewServer()
	server := &extensionServer{
		serviceFQDN: serviceFQDN,
		servicePort: servicePort,
	}
	egextension.RegisterEnvoyGatewayExtensionServer(srv, server)
	grpc_health_v1.RegisterHealthServer(srv, &healthServer{})

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Printf("custom extension server listening on %s (ext_proc target: %s:%d)", addr, serviceFQDN, servicePort)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

type healthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (h *healthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// extensionServer implements the EnvoyGatewayExtensionServer interface,
// handling only PostTranslateModify.
type extensionServer struct {
	egextension.UnimplementedEnvoyGatewayExtensionServer
	serviceFQDN string
	servicePort uint32
}

func (s *extensionServer) PostTranslateModify(_ context.Context, req *egextension.PostTranslateModifyRequest) (*egextension.PostTranslateModifyResponse, error) {
	// 1. Ensure the STRICT_DNS cluster for the custom ext_proc service exists.
if !clusterExists(req.Clusters, customExtProcClusterName) {
		cluster, err := buildCustomExtProcCluster(s.serviceFQDN, s.servicePort)
		if err != nil {
			return nil, fmt.Errorf("failed to build custom extproc cluster: %w", err)
		}
		req.Clusters = append(req.Clusters, cluster)
		log.Printf("added cluster %s -> %s:%d", customExtProcClusterName, s.serviceFQDN, s.servicePort)
	}

	// 2. Insert Composite filter into listeners that contain the AI Gateway ext_proc.
	for _, listener := range req.Listeners {
		if err := insertCompositeIntoListener(listener); err != nil {
			log.Printf("warning: failed to patch listener %s: %v", listener.Name, err)
		}
	}

	// 3. Enable the Composite filter on AI-gateway-generated routes.
	for _, routeCfg := range req.Routes {
		enableCompositeOnAIGatewayRoutes(routeCfg)
	}

	return &egextension.PostTranslateModifyResponse{
		Clusters:  req.Clusters,
		Secrets:   req.Secrets,
		Listeners: req.Listeners,
		Routes:    req.Routes,
	}, nil
}

func (s *extensionServer) PostClusterModify(_ context.Context, req *egextension.PostClusterModifyRequest) (*egextension.PostClusterModifyResponse, error) {
	return &egextension.PostClusterModifyResponse{Cluster: req.Cluster}, nil
}

func (s *extensionServer) PostRouteModify(_ context.Context, req *egextension.PostRouteModifyRequest) (*egextension.PostRouteModifyResponse, error) {
	return &egextension.PostRouteModifyResponse{Route: req.Route}, nil
}

func clusterExists(clusters []*clusterv3.Cluster, name string) bool {
	for _, c := range clusters {
		if c.Name == name {
			return true
		}
	}
	return false
}

func buildCustomExtProcCluster(fqdn string, port uint32) (*clusterv3.Cluster, error) {
	po := &httpv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{
					Http2ProtocolOptions: &corev3.Http2ProtocolOptions{},
				},
			},
		},
	}
	poAny, err := toAny(po)
	if err != nil {
		return nil, err
	}

	return &clusterv3.Cluster{
		Name:                 customExtProcClusterName,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_STRICT_DNS},
		ConnectTimeout:       durationpb.New(5 * time.Second),
		TypedExtensionProtocolOptions: map[string]*anypb.Any{
			"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": poAny,
		},
		PerConnectionBufferLimitBytes: wrapperspb.UInt32(52428800),
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: customExtProcClusterName,
			Endpoints: []*endpointv3.LocalityLbEndpoints{{
				LbEndpoints: []*endpointv3.LbEndpoint{{
					HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
						Endpoint: &endpointv3.Endpoint{
							Address: &corev3.Address{
								Address: &corev3.Address_SocketAddress{
									SocketAddress: &corev3.SocketAddress{
										Protocol: corev3.SocketAddress_TCP,
										Address:  fqdn,
										PortSpecifier: &corev3.SocketAddress_PortValue{
											PortValue: port,
										},
									},
								},
							},
						},
					},
				}},
			}},
		},
	}, nil
}

func insertCompositeIntoListener(listener *listenerv3.Listener) error {
	filterChains := listener.GetFilterChains()
	if listener.DefaultFilterChain != nil {
		filterChains = append(filterChains, listener.DefaultFilterChain)
	}
	for _, chain := range filterChains {
		hcm, hcmIdx, err := findHCM(chain)
		if err != nil {
			continue
		}
		if filterAlreadyPresent(hcm.HttpFilters, compositeFilterName) {
			continue
		}
		aigwIdx := findFilterIndex(hcm.HttpFilters, aiGatewayExtProcName)
		if aigwIdx == -1 {
			continue
		}

		compositeFilter, err := buildCompositeHTTPFilter()
		if err != nil {
			return fmt.Errorf("build composite filter: %w", err)
		}

		// Insert the Composite filter immediately before the AI Gateway ext_proc.
		hcm.HttpFilters = append(hcm.HttpFilters, nil)
		copy(hcm.HttpFilters[aigwIdx+1:], hcm.HttpFilters[aigwIdx:])
		hcm.HttpFilters[aigwIdx] = compositeFilter

		hcmAny, err := toAny(hcm)
		if err != nil {
			return fmt.Errorf("marshal HCM: %w", err)
		}
		chain.Filters[hcmIdx].ConfigType = &listenerv3.Filter_TypedConfig{TypedConfig: hcmAny}
	}
	return nil
}

// buildCompositeHTTPFilter constructs the Composite HTTP filter. The matcher
// checks for the opt-in header; on match it executes the custom ext_proc via
// ExecuteFilterAction.
func buildCompositeHTTPFilter() (*httpconnectionmanagerv3.HttpFilter, error) {
	extProc := &extprocv3.ExternalProcessor{
		GrpcService: &corev3.GrpcService{
			TargetSpecifier: &corev3.GrpcService_EnvoyGrpc_{
				EnvoyGrpc: &corev3.GrpcService_EnvoyGrpc{
					ClusterName: customExtProcClusterName,
				},
			},
			Timeout: durationpb.New(30 * time.Second),
		},
		ProcessingMode: &extprocv3.ProcessingMode{
			RequestHeaderMode:  extprocv3.ProcessingMode_SEND,
			RequestBodyMode:    extprocv3.ProcessingMode_BUFFERED,
			ResponseHeaderMode: extprocv3.ProcessingMode_SEND,
			ResponseBodyMode:   extprocv3.ProcessingMode_BUFFERED,
		},
		MessageTimeout:    durationpb.New(15 * time.Second),
		FailureModeAllow:  false,
		AllowModeOverride: true,
	}
	extProcAny, err := toAny(extProc)
	if err != nil {
		return nil, err
	}

	action := &compositev3.ExecuteFilterAction{
		TypedConfig: &corev3.TypedExtensionConfig{
			Name:        nestedExtProcName,
			TypedConfig: extProcAny,
		},
	}
	actionAny, err := toAny(action)
	if err != nil {
		return nil, err
	}

	headerInput := &matcherv3.HttpRequestHeaderMatchInput{
		HeaderName: optInHeaderName,
	}
	headerInputAny, err := toAny(headerInput)
	if err != nil {
		return nil, err
	}

	xdsMatcher := &xdsmatcherv3.Matcher{
		MatcherType: &xdsmatcherv3.Matcher_MatcherList_{
			MatcherList: &xdsmatcherv3.Matcher_MatcherList{
				Matchers: []*xdsmatcherv3.Matcher_MatcherList_FieldMatcher{{
					Predicate: &xdsmatcherv3.Matcher_MatcherList_Predicate{
						MatchType: &xdsmatcherv3.Matcher_MatcherList_Predicate_SinglePredicate_{
							SinglePredicate: &xdsmatcherv3.Matcher_MatcherList_Predicate_SinglePredicate{
								Input: &xdscorev3.TypedExtensionConfig{
									Name:        "envoy.matching.inputs.request_headers",
									TypedConfig: headerInputAny,
								},
								Matcher: &xdsmatcherv3.Matcher_MatcherList_Predicate_SinglePredicate_ValueMatch{
									ValueMatch: &xdsmatcherv3.StringMatcher{
										MatchPattern: &xdsmatcherv3.StringMatcher_Exact{
											Exact: optInHeaderValue,
										},
									},
								},
							},
						},
					},
					OnMatch: &xdsmatcherv3.Matcher_OnMatch{
						OnMatch: &xdsmatcherv3.Matcher_OnMatch_Action{
							Action: &xdscorev3.TypedExtensionConfig{
								Name:        "composite-action",
								TypedConfig: actionAny,
							},
						},
					},
				}},
			},
		},
	}

	// Empty Composite config; the matcher lives on ExtensionWithMatcher
	// (Composite.Matcher is not-implemented in Envoy).
	compositeAny, err := toAny(&compositev3.Composite{})
	if err != nil {
		return nil, err
	}

	ewm := &commonmatchingv3.ExtensionWithMatcher{
		XdsMatcher: xdsMatcher,
		ExtensionConfig: &corev3.TypedExtensionConfig{
			Name:        "composite",
			TypedConfig: compositeAny,
		},
	}
	ewmAny, err := toAny(ewm)
	if err != nil {
		return nil, err
	}

	return &httpconnectionmanagerv3.HttpFilter{
		Name:       compositeFilterName,
		Disabled:   true,
		ConfigType: &httpconnectionmanagerv3.HttpFilter_TypedConfig{TypedConfig: ewmAny},
	}, nil
}

func enableCompositeOnAIGatewayRoutes(routeCfg *routev3.RouteConfiguration) {
	for _, vh := range routeCfg.VirtualHosts {
		for _, route := range vh.Routes {
			if !isRouteGeneratedByAIGateway(route) {
				continue
			}
			if route.TypedPerFilterConfig == nil {
				route.TypedPerFilterConfig = make(map[string]*anypb.Any)
			}
			if _, exists := route.TypedPerFilterConfig[compositeFilterName]; exists {
				continue
			}
			fc := &routev3.FilterConfig{Config: &anypb.Any{}}
			fcAny, err := toAny(fc)
			if err != nil {
				log.Printf("warning: marshal FilterConfig: %v", err)
				continue
			}
			route.TypedPerFilterConfig[compositeFilterName] = fcAny
		}
	}
}

func isRouteGeneratedByAIGateway(route *routev3.Route) bool {
	if route.Metadata == nil || route.Metadata.FilterMetadata == nil {
		return false
	}
	eg, ok := route.Metadata.FilterMetadata["envoy-gateway"]
	if !ok {
		return false
	}
	resources, ok := eg.Fields["resources"]
	if !ok || resources.GetListValue() == nil {
		return false
	}
	for _, resource := range resources.GetListValue().Values {
		s := resource.GetStructValue()
		if s == nil {
			continue
		}
		annotations, ok := s.Fields["annotations"]
		if !ok || annotations.GetStructValue() == nil {
			continue
		}
		if _, ok := annotations.GetStructValue().Fields["ai-gateway-generated"]; ok {
			return true
		}
	}
	return false
}

func findHCM(filterChain *listenerv3.FilterChain) (*httpconnectionmanagerv3.HttpConnectionManager, int, error) {
	if filterChain == nil {
		return nil, -1, fmt.Errorf("nil filter chain")
	}
	for i, f := range filterChain.Filters {
		if f.Name == "envoy.filters.network.http_connection_manager" {
			hcm := &httpconnectionmanagerv3.HttpConnectionManager{}
			if err := f.GetTypedConfig().UnmarshalTo(hcm); err != nil {
				return nil, -1, err
			}
			return hcm, i, nil
		}
	}
	return nil, -1, fmt.Errorf("no HCM in filter chain %s", filterChain.Name)
}

func findFilterIndex(filters []*httpconnectionmanagerv3.HttpFilter, name string) int {
	for i, f := range filters {
		if f.Name == name {
			return i
		}
	}
	return -1
}

func filterAlreadyPresent(filters []*httpconnectionmanagerv3.HttpFilter, name string) bool {
	return findFilterIndex(filters, name) >= 0
}

func toAny(msg proto.Message) (*anypb.Any, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	const prefix = "type.googleapis.com/"
	return &anypb.Any{
		TypeUrl: prefix + string(msg.ProtoReflect().Descriptor().FullName()),
		Value:   b,
	}, nil
}
