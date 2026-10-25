# Running on Kubernetes

The manifests are meant for a local cluster (kind or minikube). `k8s/infra` holds **dev-grade** Postgres, Redis, Kafka and Jaeger. In production, replace them with managed services or operators (CloudNativePG, Strimzi).

## 1. Cluster and images

```bash
kind create cluster --name ledgerflow
for s in ledger payments notifications psp-simulator; do
  docker build --build-arg SERVICE=$s -t ledgerflow/$s:local .
  kind load docker-image ledgerflow/$s:local --name ledgerflow
done
```
For minikube, run `eval $(minikube docker-env)` before `docker build` and skip `kind load`.

## 2. KEDA (autoscaling on Kafka lag)

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda -n keda --create-namespace
```
For CPU-based triggers you also need metrics-server. On kind:
`kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml`, then patch it with `--kubelet-insecure-tls`.

## 3. Deploy

```bash
kubectl apply -k k8s/infra
kubectl -n ledgerflow rollout status statefulset/postgres
kubectl -n ledgerflow rollout status statefulset/kafka
kubectl apply -k k8s/base
kubectl -n ledgerflow get pods -w
```

## 4. Use it

```bash
kubectl -n ledgerflow port-forward svc/ledger 3001 &
kubectl -n ledgerflow port-forward svc/payments 3002 &
kubectl -n ledgerflow port-forward svc/notifications 3003 &
npm run e2e
```

## 5. Watch autoscaling

```bash
k6 run -e RATE=300 -e DURATION=3m load/payments.js
kubectl -n ledgerflow get scaledobject,hpa,pods -w
```
As lag on `ledger.commands` passes 100 messages per replica, KEDA adds ledger pods, up to 6 (the partition count). When lag drains, it scales back down after the 120 s cooldown.

## What the manifests show

- `readinessProbe` on `/ready` checks Postgres and Redis, so traffic only reaches pods that can serve it. `livenessProbe` on `/health` only checks that the process is alive. Never put dependency checks in liveness, or a database blip restarts every pod.
- `maxUnavailable: 0` rolling updates plus a PodDisruptionBudget mean no downtime during deploys or node drains.
- `terminationGracePeriodSeconds: 30` plus Nest shutdown hooks: on SIGTERM the consumer leaves the group cleanly, the outbox relay finishes its batch, and pools close.
- Non-root, read-only root filesystem, no privilege escalation.
- Config lives in a ConfigMap and secrets in a Secret. In a real cluster, use External Secrets or Sealed Secrets.
