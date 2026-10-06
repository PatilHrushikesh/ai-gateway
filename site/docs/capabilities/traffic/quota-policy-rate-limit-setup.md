---
id: quota-policy-rate-limit-setup
title: Quota Policy Rate-Limit Service Setup
sidebar_position: 7
---

# Quota Policy Rate-Limit Service Setup

`QuotaPolicy` resources are reconciled by the AI Gateway controller, but quota
enforcement requires a separate Envoy rate-limit service. The controller
publishes `RateLimitConfig` resources over its quota xDS endpoint; the
rate-limit service consumes those resources and stores counters in Redis.

The generic Envoy Gateway rate-limit service is not interchangeable with this
service. It uses a different xDS server and node ID for usage-based rate
limiting.

## Components

Quota enforcement requires:

1. The `QuotaPolicy` CRD and AI Gateway controller.
2. A Redis-compatible store. Redis Sentinel is supported.
3. A dedicated Envoy rate-limit deployment.
4. A ClusterIP service named according to the controller's
   `quotaRateLimitServiceAddr`.

The controller must expose its quota xDS port, normally `18002`, and the
rate-limit service must use node ID `envoy-ai-gateway-ratelimit`.

## Install the dedicated service

The repository includes a deployment manifest:

```bash
kubectl apply -f manifests/quota-policy/envoy-ai-gateway-ratelimit.yaml
kubectl -n envoy-gateway-system rollout status \
  deployment/envoy-ai-gateway-ratelimit
```

Before applying it to another cluster, update these values:

| Setting | Required value |
| --- | --- |
| `CONFIG_GRPC_XDS_SERVER_URL` | `<controller-service>.<controller-namespace>.svc.cluster.local:18002` |
| `CONFIG_GRPC_XDS_NODE_ID` | `envoy-ai-gateway-ratelimit` |
| `REDIS_TYPE` | `sentinel` when using Redis/Valkey Sentinel |
| `REDIS_URL` | `<master-name>,<sentinel-service>.<namespace>.svc.cluster.local:26379` |
| Service name | The host configured by `quotaRateLimitServiceAddr` |
| Service port | `8081` |

For a standalone Redis instance, omit `REDIS_TYPE=sentinel` and set
`REDIS_URL` to the Redis service and port.

## Configure the controller

Set the controller Helm values to the same rate-limit service address:

```yaml
controller:
  quotaRateLimitServiceAddr: envoy-ai-gateway-ratelimit.envoy-gateway-system
  quotaRateLimitTimeout: 5
  quotaRateLimitFailureModeDeny: false
```

The address may include a port. If it does not, the controller uses port
`8081`. Use `quotaRateLimitFailureModeDeny: true` when quota enforcement must
fail closed if the rate-limit service is unavailable. The default is `false`,
which fails open.

## Verify the installation

Check the CRD, controller, service, and xDS-related configuration:

```bash
kubectl get crd quotapolicies.aigateway.envoyproxy.io
kubectl -n nai-system get svc ai-gateway-controller
kubectl -n envoy-gateway-system get deploy,svc envoy-ai-gateway-ratelimit
kubectl -n envoy-gateway-system logs deploy/envoy-ai-gateway-ratelimit
```

The rate-limit logs should show a connection to the controller's xDS endpoint
and configuration updates for node `envoy-ai-gateway-ratelimit`. The controller
logs should show:

```text
updated rate limit xDS snapshot
```

Verify that a policy is accepted:

```bash
kubectl get quotapolicy -A
kubectl describe quotapolicy <name> -n <namespace>
```

`Accepted` confirms reconciliation and descriptor generation. It does not by
itself prove that request enforcement is active.

## Configure a policy correctly

The `targetRefs` entry must identify an `AIServiceBackend`. If the policy and
backend are in different namespaces, set `targetRefs[].namespace` explicitly:

```yaml
apiVersion: aigateway.envoyproxy.io/v1alpha1
kind: QuotaPolicy
metadata:
  name: provider-budget
  namespace: platform
spec:
  targetRefs:
    - group: aigateway.envoyproxy.io
      kind: AIServiceBackend
      name: provider
      namespace: providers
  perModelQuotas:
    - modelName: my-model
      quota:
        defaultBucket:
          limit: 10000
          duration: 1h
```

`modelName` must exactly match the route backend reference's
`modelNameOverride`. A policy for `my-model` does not apply to a route using
`my-model-alias`.

For a bucket per distinct header value, use `Distinct`:

```yaml
bucketRules:
  - clientSelectors:
      - headers:
          - name: x-tenant-id
            type: Distinct
    quota:
      limit: 5000
      duration: 1h
```

Use `Exact` only when a `value` is supplied for a single known header value.
Only `1s`, `1m`, `1h`, and `1d` are valid quota durations. Currently,
`Shared` is the only supported bucket mode.

The `serviceQuota` API field is not currently enforced. Use
`perModelQuotas` for quotas that must affect traffic.

## ReferenceGrant positive and negative tests

Run these tests in a disposable namespace. The backend namespace owns the
`ReferenceGrant`; the policy namespace never owns or creates the grant.

### Positive: same-namespace target

Create an `AIServiceBackend` and a `QuotaPolicy` in the same namespace, and
omit `targetRefs[].namespace`:

```yaml
apiVersion: aigateway.envoyproxy.io/v1alpha1
kind: QuotaPolicy
metadata:
  name: local-budget
  namespace: platform
spec:
  targetRefs:
    - group: aigateway.envoyproxy.io
      kind: AIServiceBackend
      name: provider
  perModelQuotas:
    - modelName: provider-model
      quota:
        defaultBucket:
          limit: 100
          duration: 1h
```

Expected result: the policy is `Accepted`. No `ReferenceGrant` is required.
Setting `targetRefs[].namespace: platform` explicitly has the same result.

### Positive: authorized cross-namespace target

Create a policy in `platform` that targets `providers/provider`, then create
this grant in `providers`:

```yaml
apiVersion: gateway.networking.k8s.io/v1beta1
kind: ReferenceGrant
metadata:
  name: allow-platform-quota
  namespace: providers
spec:
  from:
    - group: aigateway.envoyproxy.io
      kind: QuotaPolicy
      namespace: platform
  to:
    - group: aigateway.envoyproxy.io
      kind: AIServiceBackend
      name: provider
```

Expected result: the policy becomes `Accepted`, the quota descriptor contains
`backend_name=providers/provider`, and the dedicated rate-limit service logs a
matching `ai-gateway-quota` descriptor. Omitting `to.name` is also positive
and authorizes every `AIServiceBackend` in `providers`.

### Negative: no grant or invalid grant

For a policy in `platform` targeting `providers/provider`, each of the
following must result in `NotAccepted` with reason `RefNotPermitted`:

- no `ReferenceGrant` in `providers`;
- `from.group` is not `aigateway.envoyproxy.io`;
- `from.kind` is not `QuotaPolicy`;
- `from.namespace` is not `platform`;
- `to.group` is not `aigateway.envoyproxy.io`;
- `to.kind` is not `AIServiceBackend`;
- `to.name` names a different backend;
- the `ReferenceGrant` has a deletion timestamp.

The controller must reject the reference before looking up the remote
`AIServiceBackend`. The policy status may describe the denied relationship,
but must not disclose whether an unauthorized backend exists. The denied
target must not produce quota xDS descriptors, route cost expressions,
ext-proc quota metadata, or rate-limit actions.

### Mixed and lifecycle tests

Use a policy with one local/authorized target and one unauthorized remote
target. The authorized target must continue to contribute quota configuration,
while the policy reports `NotAccepted/RefNotPermitted` for the denied target.

Then perform these transitions and check status, controller logs, xDS, and
rate-limit logs after each transition:

1. Create a valid grant: the affected policy reconciles and becomes
   `Accepted`.
2. Narrow `to.name` so it no longer matches: the policy becomes
   `NotAccepted` and the remote descriptor is removed.
3. Broaden `to` by removing `name`: the policy becomes `Accepted` again.
4. Delete the grant: the policy becomes `NotAccepted` and stale enforcement is
   removed.
5. Recreate the grant: the policy becomes `Accepted` and enforcement returns.
6. Restart the controller while the grant is absent: no stale quota
   configuration may remain after reconciliation.

ReferenceGrant changes must enqueue only QuotaPolicies targeting the changed
backend namespace. A route-to-backend grant is independent: it authorizes an
`AIGatewayRoute` reference and does not authorize a QuotaPolicy reference.

## Test enforcement

Use a route whose `modelNameOverride` matches the policy and configure a small
temporary quota, for example `limit: 1` with `duration: 1h`. Send requests
through the matching gateway route and verify that a request exceeding the
available token budget receives HTTP `429`.

After testing, restore the intended quota. Do not use a zero-value
`serviceQuota` as a test: service-wide quotas are not currently enforced.

For each positive data-plane test, send at least two verbose requests:

```bash
curl -k -v "$GATEWAY_URL" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -H "x-ai-eg-model: provider-model" \
  -d '{"model":"provider-model","messages":[{"role":"user","content":"hi"}]}'
```

Verify HTTP success, `x-ratelimit-*` headers, and a rate-limit log containing
the expected backend/model descriptor and `hits_addend`. For an intentionally
exhausted quota, verify HTTP `429` and `overall_code:OVER_LIMIT` (or the
equivalent rate-limit response). For a negative authorization test, verify
that requests do not contain a descriptor for the unauthorized
`namespace/backend` identity.

## Troubleshooting

### Policy is `Accepted`, but requests are not limited

Check that:

- The dedicated service exists as
  `envoy-ai-gateway-ratelimit` in `envoy-gateway-system`.
- Its `CONFIG_GRPC_XDS_NODE_ID` is exactly
  `envoy-ai-gateway-ratelimit`.
- Its xDS URL points to the AI Gateway controller on port `18002`.
- Its `REDIS_URL` points to a reachable Redis/Valkey endpoint.
- The controller's `quotaRateLimitServiceAddr` matches the service DNS name.
- The route's `modelNameOverride` matches the policy's `modelName`.
- The request reaches an `AIGatewayRoute` with quota-enabled backend metadata.

If `quotaRateLimitFailureModeDeny` is `false`, a missing or unhealthy
rate-limit service causes requests to continue without quota enforcement.

### Rate-limit service cannot connect to xDS

Check service discovery and endpoints:

```bash
kubectl -n nai-system get svc,endpointslice ai-gateway-controller
kubectl -n envoy-gateway-system get svc,endpointslice envoy-ai-gateway-ratelimit
```

The controller service must expose port `18002`, and the rate-limit service
must expose gRPC port `8081`.

### Rate-limit service cannot connect to Sentinel

Check the Sentinel service and its master name:

```bash
kubectl -n nai-system get svc nai-valkey-sentinel
kubectl -n nai-system get configmap nai-valkey-config -o yaml
```

The first component of `REDIS_URL` must match the Sentinel master name.
