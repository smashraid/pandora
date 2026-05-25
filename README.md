# LoanFlow: A gRPC‑Powered Reconciliation Engine for High‑Volume Loan Applications

[![Go Report Card](https://goreportcard.com/badge/github.com/smashraid/pandora)](https://goreportcard.com/report/github.com/smashraid/pandora)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Buf](https://img.shields.io/badge/buf-lint%20%7C%20format-important)](https://buf.build)

**Fintech document processing pipeline** – asynchronous, real‑time progress, and cancellable.  
Built with Go, gRPC bidirectional streams, and a Redis/PostgreSQL backend.

> *“Every Monday morning, our loan origination system would buckle under the load. HTTP timeouts spiked, loan officers stared at loading spinners, and ops couldn’t tell if a job was stuck or just slow. We needed to process 10,000+ applications with p99 < 90s, real‑time visibility, and the ability to cancel a stuck credit check before we got double‑billed.”*  
> — **Sarah, Head of Platform Engineering at ‘FastFund Lending’**

---

## 📌 The Problem (Business Context)

A fast‑growing lending platform receives thousands of loan applications daily – PDF bank statements, payslips, ID scans. Their **synchronous REST pipeline** suffers:

| Issue | Business Impact |
|-------|------------------|
| ❌ **60s timeouts** – long OCR & credit bureau calls get dropped | Lost applications, angry customers |
| ❌ **No progress visibility** – “is it stuck or working?” | Wasted underwriter time |
| ❌ **No cancellation** – duplicate submission can’t stop an ongoing expensive API call | Paying twice for the same credit check |
| ❌ **No load shedding** – Monday morning spike crashes the service | Reputation loss, SLA breach |

**Business goal:** Process 500 concurrent loan apps with **p99 < 90s**, provide real‑time status, and reduce timeout errors by **>90%**.

---

## 🚀 How We Solve It – gRPC Async Task Scheduler

We built a **distributed task system** using **gRPC bidirectional streaming** + **Redis broker** + **PostgreSQL state store**.

Key decisions:

1. **Async submission** – Client gets a `task_id` immediately → no blocking.
2. **Bidirectional stream** – Server pushes incremental progress (e.g., “OCR page 5/20”, “calling Credit Bureau”). Client can **cancel** mid‑stream.
3. **Priority queues** – High‑value loans (>500k) go to a fast lane in Redis.
4. **Idempotency** – Same `application_id` never processed twice.
5. **Observability** – OpenTelemetry traces show exactly where time is spent (OCR, external API, DB).
6. **Graceful degradation** – When overloaded, server rejects new tasks with `RESOURCE_EXHAUSTED` (gRPC status code 8).

### Why gRPC over REST + WebSocket?

- **HTTP/2 multiplexing** – manage thousands of concurrent streams efficiently.
- **Built‑in deadline propagation** – client cancellation cancels context all the way down to DB queries, saving resources.
- **Strongly typed contracts** with Protobuf – no runtime JSON surprises.
- **Native streaming** – single connection for both progress updates and cancellation commands.

---

## 🧱 Architecture

```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'primaryColor': 'var(--color-canvas-subtle)',
    'primaryTextColor': 'var(--color-fg-default)',
    'primaryBorderColor': 'var(--color-border-default)',
    'lineColor': 'var(--color-accent-fg)',
    'secondaryColor': 'var(--color-neutral-subtle)',
    'tertiaryColor': 'var(--color-canvas-default)'
  }
}}%%
graph TD
    Client([Loan Officer Client]) -->|1. Submit Application| Scheduler

    subgraph Scheduler [LoanFlow Scheduler]
        SubmitAPI[SubmitApplication Unary API]
        StreamAPI[TrackProgress BidiStream API]
    end

    subgraph Workers [Worker Pool]
        Worker1[OCR Worker]
        Worker2[Validation Worker]
        Worker3[Credit Bureau Worker]
    end

    subgraph Storage [Persistence & Queue]
        Postgres[(PostgreSQL - Task State)]
        Redis[(Redis - Task Queue & Pub/Sub)]
    end

    SubmitAPI -->|2. Create Task| Postgres
    SubmitAPI -->|3. Enqueue Task ID| Redis
    Redis -.->|4. Claim Task| Worker1

    Worker1 -->|5. Process| Worker2 -->|6. Process| Worker3
    Worker3 -->|7. Update Final Result| Postgres

    Worker1 -.->|Emit Progress Events| Redis
    Redis -.->|Forward Progress Events| StreamAPI
    StreamAPI -.->|8. Send Real-time Updates| Client

    Client -.->|9. Cancel| StreamAPI

    %% Dynamic Contrast Styling %%
    style Client fill:var(--color-accent-subtle),stroke:var(--color-accent-fg),stroke-width:2px
    style Postgres fill:var(--color-done-subtle),stroke:var(--color-done-fg),stroke-width:1px
    style Redis fill:var(--color-attention-subtle),stroke:var(--color-attention-fg),stroke-width:1px

```

**Data flow:**

1. **Submit** – scheduler validates proto, stores task state `PENDING` in Postgres, pushes task ID to Redis queue → returns `task_id`.
2. **Stream** – client opens a `TrackProgress` stream. Server subscribes to Redis pub/sub for that task ID and forwards every progress event.
3. **Cancel** – client sends `CancelCommand` on the same stream → server cancels the context → worker aborts.
4. **Worker** – claims task from Redis, updates progress to Postgres & publishes events to Redis Pub/Sub.

All gRPC calls are secured with **mTLS** + JWT (auth interceptor).

---

## ✨ Best Practices & Patterns

| Pattern | Implementation |
|---------|----------------|
| **Async submission** | Client receives task_id immediately; no blocking.
| **Bidirectional streaming** | Real‑time progress + cancellation over one channel |
| **Prioritized queue** | Redis lists: `high` and `standard`; workers with semaphore prefer `high`. |
| **Idempotency** | DB unique constraint on `application_id`; duplicate returns existing `task_id`. |
| **Graceful shutdown** | Rate limiter + worker semaphore. Overloaded → `codes.ResourceExhausted`. |
| **Context propagation** | From client `context.WithCancel` → server → worker → DB calls |
| **Distributed tracing** | OpenTelemetry → Jaeger spans for gRPC calls, Redis dequeue, external API mock. |
| **Cancellation propagation** | `context.WithCancel` from client → server → worker → DB calls. |
| **Configuration** | `envconfig` + default values; no hardcoded secrets |

---

## 🛠 Tech Stack

- **Language:** Go 1.21+  
- **gRPC:** `google.golang.org/grpc` v1.60+  
- **Protocol Buffers:** `buf` for linting & breaking change detection, `protovalidate` for runtime validation  
- **Gateway:** `grpc-gateway` v2 (REST + Swagger UI)  
- **Message Broker:** Redis (for task queue + pub/sub)  
- **Database:** PostgreSQL (task state, audit log)  
- **Observability:** OpenTelemetry → Jaeger (traces), Prometheus (metrics)  
- **Deployment:** Docker + Kubernetes (gRPC load balancing via headless service)  
- **CI/CD:** GitHub Actions – `buf lint`, unit tests, integration tests, Docker build, push to GHCR

---

## 📦 Getting Started (local development)

### Prerequisites
- Go 1.21+, Docker (Postgres & Redis)
- `buf` CLI – [install](https://buf.build/docs/installation)
- `protoc-gen-go`, `protoc-gen-go-grpc`, `protoc-gen-grpc-gateway`

### 1. Clone & generate proto
```bash
git clone https://github.com/yourusername/loanflow
cd loanflow
make proto   # runs buf generate
```

### 2. Start dependencies
```bash
docker-compose up -d postgres redis jaeger
```

### 3. Run server
```bash
go run cmd/scheduler/main.go
```

### 4. Interact (using grpcurl)
```bash
# Submit a loan application
grpcurl -plaintext -d '{"application_id":"loan-123", "priority":"HIGH"}' \
  localhost:5000 loanflow.LoanDocumentProcessor/SubmitApplication

# Open a progress stream (in another terminal)
grpcurl -plaintext -d '{"task_id":"<returned_id>"}' \
  localhost:5000 loanflow.LoanDocumentProcessor/TrackProgress
```

### 5. Load test with ghz
```bash
ghz --insecure --proto api/loanflow.proto --call loanflow.LoanDocumentProcessor/SubmitApplication \
  -d '{"application_id":"{{.RequestNumber}}","priority":"STANDARD"}' \
  -n 500 -c 50 localhost:5000
```

