package main

import (
	"context"
	"testing"

	egextension "github.com/envoyproxy/gateway/proto/extension"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	httpconnectionmanagerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func buildTestListener(t *testing.T, withAIGatewayExtProc bool) *listenerv3.Listener {
	t.Helper()
	hcm := &httpconnectionmanagerv3.HttpConnectionManager{
		HttpFilters: []*httpconnectionmanagerv3.HttpFilter{
			{Name: "envoy.filters.http.buffer"},
		},
	}
	if withAIGatewayExtProc {
		epAny, err := toAny(&extprocv3.ExternalProcessor{
			GrpcService: &corev3.GrpcService{
				TargetSpecifier: &corev3.GrpcService_EnvoyGrpc_{
					EnvoyGrpc: &corev3.GrpcService_EnvoyGrpc{ClusterName: "ai-gateway-extproc-uds"},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		hcm.HttpFilters = append(hcm.HttpFilters,
			&httpconnectionmanagerv3.HttpFilter{
				Name:       aiGatewayExtProcName,
				Disabled:   true,
				ConfigType: &httpconnectionmanagerv3.HttpFilter_TypedConfig{TypedConfig: epAny},
			},
			&httpconnectionmanagerv3.HttpFilter{Name: "envoy.filters.http.router"},
		)
	} else {
		hcm.HttpFilters = append(hcm.HttpFilters,
			&httpconnectionmanagerv3.HttpFilter{Name: "envoy.filters.http.router"},
		)
	}

	hcmAny, err := toAny(hcm)
	if err != nil {
		t.Fatal(err)
	}
	return &listenerv3.Listener{
		Name: "test-listener",
		FilterChains: []*listenerv3.FilterChain{{
			Name: "test-chain",
			Filters: []*listenerv3.Filter{{
				Name:       "envoy.filters.network.http_connection_manager",
				ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: hcmAny},
			}},
		}},
	}
}

func buildAIGatewayRoute() *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: "test-route-config",
		VirtualHosts: []*routev3.VirtualHost{{
			Name: "test-vh",
			Routes: []*routev3.Route{
				{
					Name: "httproute/default/my-ai-route/rule/0/match/0",
					Metadata: &corev3.Metadata{
						FilterMetadata: map[string]*structpb.Struct{
							"envoy-gateway": {
								Fields: map[string]*structpb.Value{
									"resources": structpb.NewListValue(&structpb.ListValue{
										Values: []*structpb.Value{
											structpb.NewStructValue(&structpb.Struct{
												Fields: map[string]*structpb.Value{
													"annotations": structpb.NewStructValue(&structpb.Struct{
														Fields: map[string]*structpb.Value{
															"ai-gateway-generated": structpb.NewStringValue("true"),
														},
													}),
												},
											}),
										},
									}),
								},
							},
						},
					},
				},
				{
					Name:     "httproute/default/other-route/rule/0/match/0",
					Metadata: &corev3.Metadata{},
				},
			},
		}},
	}
}

func TestPostTranslateModify_AddsCluster(t *testing.T) {
	server := &extensionServer{
		serviceFQDN: "custom-extproc.test.svc.cluster.local",
		servicePort: 9000,
	}
	req := &egextension.PostTranslateModifyRequest{
		Clusters:  []*clusterv3.Cluster{},
		Listeners: []*listenerv3.Listener{},
		Routes:    []*routev3.RouteConfiguration{},
	}

	resp, err := server.PostTranslateModify(context.Background(), req)
	if err != nil {
		t.Fatalf("PostTranslateModify error: %v", err)
	}

	found := false
	for _, c := range resp.Clusters {
		if c.Name == customExtProcClusterName {
			found = true
			if c.GetType() != clusterv3.Cluster_STRICT_DNS {
				t.Errorf("expected STRICT_DNS, got %v", c.GetType())
			}
		}
	}
	if !found {
		t.Errorf("cluster %q not found in response", customExtProcClusterName)
	}
}

func TestPostTranslateModify_DoesNotDuplicateCluster(t *testing.T) {
	server := &extensionServer{
		serviceFQDN: "custom-extproc.test.svc.cluster.local",
		servicePort: 9000,
	}
	existing := &clusterv3.Cluster{
		Name:                 customExtProcClusterName,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_STRICT_DNS},
		ConnectTimeout:       durationpb.New(5000000000),
	}
	req := &egextension.PostTranslateModifyRequest{
		Clusters:  []*clusterv3.Cluster{existing},
		Listeners: []*listenerv3.Listener{},
		Routes:    []*routev3.RouteConfiguration{},
	}

	resp, err := server.PostTranslateModify(context.Background(), req)
	if err != nil {
		t.Fatalf("PostTranslateModify error: %v", err)
	}

	count := 0
	for _, c := range resp.Clusters {
		if c.Name == customExtProcClusterName {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 cluster %q, got %d", customExtProcClusterName, count)
	}
}

func TestPostTranslateModify_InsertsCompositeBeforeAIGateway(t *testing.T) {
	server := &extensionServer{
		serviceFQDN: "custom-extproc.test.svc.cluster.local",
		servicePort: 9000,
	}
	listener := buildTestListener(t, true)
	req := &egextension.PostTranslateModifyRequest{
		Clusters:  []*clusterv3.Cluster{},
		Listeners: []*listenerv3.Listener{listener},
		Routes:    []*routev3.RouteConfiguration{},
	}

	resp, err := server.PostTranslateModify(context.Background(), req)
	if err != nil {
		t.Fatalf("PostTranslateModify error: %v", err)
	}

	chain := resp.Listeners[0].FilterChains[0]
	hcm, _, err := findHCM(chain)
	if err != nil {
		t.Fatalf("findHCM: %v", err)
	}

	var compositeIdx, aigwIdx int
	compositeFound := false
	for i, f := range hcm.HttpFilters {
		if f.Name == compositeFilterName {
			compositeIdx = i
			compositeFound = true
			if !f.Disabled {
				t.Error("composite filter should be disabled by default")
			}
		}
		if f.Name == aiGatewayExtProcName {
			aigwIdx = i
		}
	}
	if !compositeFound {
		t.Fatal("composite filter not found in HCM")
	}
	if compositeIdx >= aigwIdx {
		t.Errorf("composite filter (idx=%d) should be before AI Gateway ext_proc (idx=%d)", compositeIdx, aigwIdx)
	}
}

func TestPostTranslateModify_SkipsListenerWithoutAIGateway(t *testing.T) {
	server := &extensionServer{
		serviceFQDN: "custom-extproc.test.svc.cluster.local",
		servicePort: 9000,
	}
	listener := buildTestListener(t, false)
	req := &egextension.PostTranslateModifyRequest{
		Clusters:  []*clusterv3.Cluster{},
		Listeners: []*listenerv3.Listener{listener},
		Routes:    []*routev3.RouteConfiguration{},
	}

	resp, err := server.PostTranslateModify(context.Background(), req)
	if err != nil {
		t.Fatalf("PostTranslateModify error: %v", err)
	}

	chain := resp.Listeners[0].FilterChains[0]
	hcm, _, err := findHCM(chain)
	if err != nil {
		t.Fatalf("findHCM: %v", err)
	}
	for _, f := range hcm.HttpFilters {
		if f.Name == compositeFilterName {
			t.Error("composite filter should NOT be inserted into listeners without AI Gateway ext_proc")
		}
	}
}

func TestPostTranslateModify_EnablesCompositeOnAIGatewayRoutes(t *testing.T) {
	server := &extensionServer{
		serviceFQDN: "custom-extproc.test.svc.cluster.local",
		servicePort: 9000,
	}
	routeCfg := buildAIGatewayRoute()
	req := &egextension.PostTranslateModifyRequest{
		Clusters:  []*clusterv3.Cluster{},
		Listeners: []*listenerv3.Listener{},
		Routes:    []*routev3.RouteConfiguration{routeCfg},
	}

	resp, err := server.PostTranslateModify(context.Background(), req)
	if err != nil {
		t.Fatalf("PostTranslateModify error: %v", err)
	}

	routes := resp.Routes[0].VirtualHosts[0].Routes

	if _, ok := routes[0].TypedPerFilterConfig[compositeFilterName]; !ok {
		t.Errorf("AI gateway route should have composite per-filter config, got keys: %v", mapKeys(routes[0].TypedPerFilterConfig))
	}

	if routes[1].TypedPerFilterConfig != nil {
		if _, ok := routes[1].TypedPerFilterConfig[compositeFilterName]; ok {
			t.Error("non-AI-gateway route should NOT have composite per-filter config")
		}
	}
}

func TestIsRouteGeneratedByAIGateway(t *testing.T) {
	tests := []struct {
		name  string
		route *routev3.Route
		want  bool
	}{
		{
			name:  "nil metadata",
			route: &routev3.Route{Name: "test"},
			want:  false,
		},
		{
			name: "no envoy-gateway key",
			route: &routev3.Route{
				Name: "test",
				Metadata: &corev3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{},
				},
			},
			want: false,
		},
		{
			name: "with ai-gateway-generated annotation",
			route: &routev3.Route{
				Name: "test",
				Metadata: &corev3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{
						"envoy-gateway": {
							Fields: map[string]*structpb.Value{
								"resources": structpb.NewListValue(&structpb.ListValue{
									Values: []*structpb.Value{
										structpb.NewStructValue(&structpb.Struct{
											Fields: map[string]*structpb.Value{
												"annotations": structpb.NewStructValue(&structpb.Struct{
													Fields: map[string]*structpb.Value{
														"ai-gateway-generated": structpb.NewStringValue("true"),
													},
												}),
											},
										}),
									},
								}),
							},
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRouteGeneratedByAIGateway(tt.route)
			if got != tt.want {
				t.Errorf("isRouteGeneratedByAIGateway() = %v, want %v", got, tt.want)
			}
		})
	}
}

func mapKeys(m map[string]*anypb.Any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
