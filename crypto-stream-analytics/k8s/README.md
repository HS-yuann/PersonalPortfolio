# Kubernetes Deployment

Deploy the crypto-stream-analytics platform to Kubernetes.

## Prerequisites

- Kubernetes cluster (1.25+)
- kubectl configured
- Docker registry access (for custom images)

## Quick Start

```bash
# Create namespace and deploy all resources
kubectl apply -k k8s/

# Verify deployment
kubectl get pods -n crypto-analytics

# Check services
kubectl get svc -n crypto-analytics
```

## Components

| Component | Replicas | Description |
|-----------|----------|-------------|
| zookeeper | 1 | Kafka coordination |
| kafka | 1 | Message broker |
| binance-producer | 1 | Binance WebSocket streamer |
| coingecko-producer | 1 | CoinGecko API poller |
| kafka-processor | 1-5 | Stream processor (auto-scales) |
| crypto-api | 2-10 | REST API (auto-scales) |
| prometheus | 1 | Metrics collection |
| grafana | 1 | Dashboards |

## Configuration

Update secrets before deploying:

```bash
# Edit secrets.yaml with your API keys
kubectl apply -f k8s/secrets.yaml
```

## Accessing Services

```bash
# Port-forward API
kubectl port-forward svc/crypto-api 8000:8000 -n crypto-analytics

# Port-forward Grafana
kubectl port-forward svc/grafana 3000:3000 -n crypto-analytics

# Port-forward Prometheus
kubectl port-forward svc/prometheus 9090:9090 -n crypto-analytics
```

## Scaling

```bash
# Manual scaling
kubectl scale deployment crypto-api --replicas=5 -n crypto-analytics

# HPA handles auto-scaling based on CPU/memory
kubectl get hpa -n crypto-analytics
```

## Cleanup

```bash
kubectl delete -k k8s/
```
