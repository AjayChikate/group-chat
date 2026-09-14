# 🟢 High-Availability Distributed Group Chat System

**Computer System Design (CSD) --- 7th Semester, 4th Year Individual Project**  
**Author**: Ajay Chikate  
**Live Load Balancer Endpoint**: [http://10.1.75.51:5297/](http://10.1.75.51:5297/)  
**GitHub Repository**: [https://github.com/AjayChikate/group-chat](https://github.com/AjayChikate/group-chat)  

[![Go](https://img.shields.io/badge/Load_Balancer-Go_1.22+-00ADD8.svg)](https://golang.org/)
[![Python](https://img.shields.io/badge/Backends-FastAPI_/_Uvicorn-009688.svg)](https://fastapi.tiangolo.com/)
[![Database](https://img.shields.io/badge/Persistence-MongoDB_Atlas-47A248.svg)](https://www.mongodb.com/)
[![Cryptography](https://img.shields.io/badge/Security-AES--256--GCM_+_Ed25519-blueviolet.svg)](#cryptographic-pipeline)
[![Architecture](https://img.shields.io/badge/Pattern-Redis--Style_Pub--Sub-orange.svg)](#redis-style-in-memory-pubsub--gossip-mesh)

---

## Table of Contents

- [Overview & Objectives](#overview--objectives)
- [Distributed System Architecture](#distributed-system-architecture)
- [System Design: What I Built](#system-design-what-we-built)
  - [1. Redis-Style In-Memory Pub/Sub & Gossip Mesh](#1-redis-style-in-memory-pubsub--gossip-mesh)
  - [2. Adaptive Go Load Balancer & Dynamic Scoring](#2-adaptive-go-load-balancer--dynamic-scoring)
  - [3. OS Hardening & Zero-Allocation Streaming](#3-os-hardening--zero-allocation-streaming)
  - [4. Cryptographic Pipeline (AES-256-GCM & Deterministic Ed25519)](#4-cryptographic-pipeline-aes-256-gcm--deterministic-ed25519)
  - [5. Decoupled Asynchronous MongoDB Atlas Persistence](#5-decoupled-asynchronous-mongodb-atlas-persistence)
- [Environment Variables Reference](#environment-variables-reference)
- [Commands to Start the System](#commands-to-start-the-system)
  - [Step 1: Prerequisites & Virtual Environment](#step-1-prerequisites--virtual-environment)
  - [Step 2: Database Initialization](#step-2-database-initialization)
  - [Step 3: Launching the 3 Backend Nodes](#step-3-launching-the-3-backend-nodes)
  - [Step 4: Compiling & Launching the Go Load Balancer](#step-4-compiling--launching-the-go-load-balancer)
  - [Step 5: Verifying Health & System Telemetry](#step-5-verifying-health--system-telemetry)
- [Project Directory Structure](#project-directory-structure)

---

## Overview & Objectives

Group Chat is a high-availability, distributed real-time chat platform engineered to withstand extreme client concurrency under a strict host-level **512 MB memory cgroup boundary**. 

Standard chat applications easily succumb to Linux Out-Of-Memory (OOM) killer terminations, goroutine/thread stack explosions, TCP socket buffer exhaustion, or slow database write round-trips. This implementation addresses these challenges by:
1. Distributing incoming load across **four decoupled subsystems** (1 Go Edge Load Balancer + 3 Horizontal FastAPI Backends + MongoDB Atlas).
2. Implementing a lightweight **Redis-style in-memory Pub/Sub message broker** across the cluster without the RAM overhead of an external Redis server.
3. Enforcing **kernel socket buffer clamping (8 KB)** and zero-alloc byte-slice streaming to cap connection memory.
4. Using **deterministic HMAC-SHA256 Ed25519 signing** to eliminate disk I/O and database key queries per message.
5. Offloading MongoDB Atlas persistence to **non-blocking background micro-batch worker threads**.

---

## Distributed System Architecture

The distributed setup consists of **4 distinct computational subsystems** plus an external cloud MongoDB Atlas database:

```
                          ┌────────────────────────┐
                          │ Virtual Users / Clients│
                          │  (HTTP REST & WSS)     │
                          └───────────┬────────────┘
                                      │ Incoming Traffic (:5297)
                                      ▼
                        ┌───────────────────────────┐
                        │   SYSTEM 1: LOAD BALANCER │
                        │  (Go Binary, EWMA Scoring,│
                        │   8KB Clamped Sockets)    │
                        └──────┬──────┬──────┬──────┘
             ┌─────────────────┘      │      └─────────────────┐
             │ :5298                  │ :5299                  │ :5300
             ▼                        ▼                        ▼
  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
  │  SYSTEM 2: BACKEND 1│  │  SYSTEM 3: BACKEND 2│  │  SYSTEM 4: BACKEND 3│
  │  (FastAPI / Uvicorn)│  │  (FastAPI / Uvicorn)│  │  (FastAPI / Uvicorn)│
  └──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘
             │                        │                        │
             ▼                        ▼                        ▼
 ═══════════════════════════════════════════════════════════════════════════════
       REDIS-STYLE IN-MEMORY PUB/SUB MESSAGE BUS & PEER GOSSIP MESH
   (Room Channel Subscriptions • Local asyncio.Queue • Micro-Batched HTTP Sync)
 ═══════════════════════════════════════════════════════════════════════════════
             │                        │                        │
             └────────────────┬───────┴────────────────────────┘
                              │ Async Micro-Batch Write Thread
                              ▼
                 ┌───────────────────────────┐
                 │    MONGODB ATLAS CLUSTER  │
                 │ (Shared 'messages' Coll)  │
                 └───────────────────────────┘
```

### Subsystem Roles
- **System 1 (Go Load Balancer)**: Edge proxy listening on `:5297`. Terminates incoming connections, enforces connection limits, tracks backend latencies via EWMA, and streams HTTP/WebSocket frames without buffering.
- **System 2 (Backend Node 1)**: FastAPI application running on port `5000` (`172.17.0.99:5000` in container topology).
- **System 3 (Backend Node 2)**: Identical FastAPI application running on port `5000` (`172.17.0.100:5000`).
- **System 4 (Backend Node 3)**: Identical FastAPI application running on port `5000` (`172.17.0.101:5000`).
- **Distributed Storage Tier**: MongoDB Atlas replica set cluster holding persistent, encrypted message documents and user public keys.

---

## System Design: 

### 1. Redis-Style In-Memory Pub/Sub & Gossip Mesh
In a multi-node deployment, when a user connected to Backend 1 posts a message to `#general`, users connected to Backend 2 and Backend 3 must receive that message in real time.

- **Why Not External Redis?**  
  Deploying a dedicated Redis instance would consume 50–100 MB of baseline memory, establish additional connection pools, and introduce an external failure domain under our 512 MB memory limit.
- **My Implementation**:
  - **Topics/Channels as Rooms**: Each chat room (`general`, `tech`, `random`) is an in-memory topic managed by `RoomManager`. Connected WebSockets subscribe to the room's `asyncio.Queue`.
  - **Local Publication**: When a message is posted, the receiving backend immediately fans out the payload to all locally connected WebSocket subscribers.
  - **Peer Gossip Mesh**: The message is simultaneously queued into a gossip worker. Outgoing messages are gathered into 20 ms micro-batches (up to 500 messages) and transmitted via persistent HTTP keep-alive connections to peer nodes (`/internal/gossip`).
  - **Self-Peer Avoidance & Deduplication**: Node hostname and network IP detection prevents instances from gossiping to themselves. Peer nodes dispatch received messages to their local subscribers without re-gossiping or writing to the database, preventing infinite loops.

### 2. Adaptive Go Load Balancer & Dynamic Scoring
Rather than naive round-robin or static weighted balancing, the Go edge proxy implements a dynamic cost scoring function:
$$S(b) = 0.40 \cdot \tilde{C}_b + 0.35 \cdot \tilde{L}_b + 0.25 \cdot \tilde{U}_b$$
- $\tilde{C}_b$: Active TCP in-flight connections on backend $b$ normalized against cluster maximum.
- $\tilde{L}_b$: Normalized Exponentially Weighted Moving Average (EWMA) latency ($L_t = 0.40 L_{\text{sample}} + 0.60 L_{t-1}$).
- $\tilde{U}_b$: Normalized CPU utilization periodically reported by backend `/metrics` endpoints.

The candidate node with the lowest score $S(b)$ is selected for routing. If a request experiences a transport dial failure or reset, the proxy automatically fails over to an alternate healthy peer—unless the client context has already timed out, suppressing retry storms.

### 3. OS Hardening & Zero-Allocation Streaming
To guarantee that the cluster stays comfortably within the 512 MB RAM limit:
- **Clamped Kernel Sockets**: The Go listener intercepts accepted TCP sockets and clamps `SO_RCVBUF` and `SO_SNDBUF` to **8192 bytes (8 KB)**. This prevents the Linux kernel from allocating default 128 KB buffers per socket, saving hundreds of megabytes of kernel RAM.
- **Zero-Allocation Stream Buffers**: Direct chunked streaming between client and backend connections utilizes a reusable `sync.Pool` of 32 KB byte slices. Full request and response bodies are never buffered in heap memory.
- **Keep-Alive Pool Synchronization**: The proxy's `IdleConnTimeout` is fixed at 30 seconds, strictly shorter than Uvicorn's 65-second timeout. This eliminates "connection reset by peer" race conditions.
- **Proactive Memory Reclamation**: A background routine triggers `debug.FreeOSMemory()` every 3 seconds to force `MADV_DONTNEED` syscalls, combined with `GOMEMLIMIT` bounding.

### 4. Cryptographic Pipeline (AES-256-GCM & Deterministic Ed25519)
Every message processed through `POST /message` or WebSockets undergoes authenticated security:
- **AES-256-GCM Encryption**: Message plaintexts are encrypted using a 256-bit master key and a cryptographically secure 96-bit random nonce ($N$).
- **Deterministic HMAC-SHA256 Ed25519 Key Derivation**:  
  Standard architectures query databases or read private key files from disk per message. Under high throughput, this creates disk and connection pool starvation.  
  Instead, signing keys are derived deterministically in RAM:
  $$\text{seed}_u = \text{HMAC-SHA256}(K_{\text{master}}, \text{"user-ed25519:"} \parallel u)$$
  This achieves sub-microsecond signing, requires zero disk I/O, and ensures identical key derivation across all backend nodes.

### 5. Decoupled Asynchronous MongoDB Atlas Persistence
Writing messages synchronously across the Internet to MongoDB Atlas introduces 30–80 ms of blocking network round-trip time.
- **In-Memory Feed Cache**: An in-memory cache of 100,000 items (`_global_msg_cache`) serves `GET /feed` requests in $<1$ ms without making database queries.
- **Decoupled Bulk Writes**: Incoming message documents are pushed non-blockingly to an in-memory queue (`queue.Queue(maxsize=100000)`). A dedicated OS daemon thread drains the queue in micro-batches of up to 500 documents and invokes MongoDB's bulk `insert_many(batch, ordered=False)`, keeping client response times under 1 ms.

---

## Environment Variables Reference

### Backend Nodes (`server/config.py` & `server/store.py`)

| Variable | Default | Explanation |
| :--- | :--- | :--- |
| `PORT` | `5000` | HTTP and WebSocket port for this backend node |
| `SERVER_ID` | `backend-unknown` | Unique node identifier (e.g., `backend1`) used in logs and metrics |
| `PEERS` | `""` | Comma-separated peer backend URLs for gossip synchronization |
| `MONGODB_URL` | Cloud Atlas URL | MongoDB Atlas connection URI with credentials and cluster address |
| `MONGODB_DB` | `group_chat` | Target database name in MongoDB |
| `DEFAULT_ROOMS` | `general,random,tech` | Comma-separated list of default chat rooms |
| `ADMIN_USERNAMES` | `admin` | Usernames granted moderator privileges (kick/mute) |
| `HISTORY_LIMIT` | `50` | Number of recent messages replayed on joining a room |
| `UVICORN_WORKERS`| `1` | Number of Uvicorn worker event loops per instance |

### Go Load Balancer (`load-balancer/main.go`)

| Variable | Default | Explanation |
| :--- | :--- | :--- |
| `LB_PORT` | `5000` | Port on which the Load Balancer listens for public client traffic |
| `BACKENDS` | `172.17.0.99..` | Comma-separated upstream backend endpoints |
| `LB_MAX_INFLIGHT`| `450` | Concurrency admission gate (set to `2500` for high concurrency) |
| `LB_THRESHOLD` | `0.70` | Node score threshold for healthy routing |
| `LB_W_CONN` | `0.40` | Scoring weight for active in-flight TCP connections |
| `LB_W_LAT` | `0.35` | Scoring weight for EWMA response latency |
| `LB_W_CPU` | `0.25` | Scoring weight for backend CPU telemetry |

---

## Commands to Start the System


### Step 1: Prerequisites & Virtual Environment

```bash
# Clone repository and navigate to root directory
git clone https://github.com/AjayChikate/group-chat.git
cd group-chat

# Create and activate Python virtual environment
python3 -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\activate

# Install backend dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---

### Step 2: Database Initialization

Clear the MongoDB Atlas collection to start with a fresh state:

```bash
#  Export MongoDB connection parameters
export MONGODB_URL="" # MongoDB Atlas connection string
export MONGODB_DB="group_chat" # Target database name

# Run the cleanup utility
python clear_db.py
```

---

### Step 3: Launching the 3 Backend Nodes

Open three separate terminal windows (or background jobs) to start each backend node with its respective peer mesh configuration:

#### Node 1 (Port 5001)
```bash
export PORT=5001  # Port for Backend 1
export SERVER_ID="backend1"   # Identifier for Backend 1
export PEERS="http://127.0.0.1:5002,http://127.0.0.1:5003"  # Peer URLs for gossip synchronization
export MONGODB_URL="" # MongoDB Atlas URI
export MONGODB_DB="group_chat"  # MongoDB database name
export UVICORN_WORKERS=1 # 1 event loop worker to bound memory

python app.py
```

#### Node 2 (Port 5002)
```bash
export PORT=5002 # Port for Backend 2
export SERVER_ID="backend2" #Identifier for Backend 2
export PEERS="http://127.0.0.1:5001,http://127.0.0.1:5003" # Peer URLs for gossip synchronization
export MONGODB_URL= "" # MongoDB Atlas URI
export MONGODB_DB="group_chat"  #MongoDB database name
export UVICORN_WORKERS=1 #1 event loop worker to bound memory

python app.py
```

#### Node 3 (Port 5003)
```bash
export PORT=5003  # Port for Backend 3
export SERVER_ID="backend3"  # Identifier for Backend 3
export PEERS="http://127.0.0.1:5001,http://127.0.0.1:5002"  # Peer URLs for gossip synchronization
export MONGODB_URL="" # MongoDB Atlas URI
export MONGODB_DB="group_chat"  #MongoDB database name
export UVICORN_WORKERS=1 # 1 event loop worker to bound memory

python app.py
```

---

### Step 4: Compiling & Launching the Go Load Balancer

In a fourth terminal, build and run the Go load balancer configured with upstream backend URLs and high-concurrency admission parameters:

```bash
cd load-balancer

#  Build the standalone optimized binary
go build -o lb main.go

# Export Load Balancer environment variables
export LB_PORT="5297"  # Public port the load balancer listens on
export BACKENDS="http://127.0.0.1:5001,http://127.0.0.1:5002,http://127.0.0.1:5003"  # Target upstream backend instances
export LB_MAX_INFLIGHT="2500"  # Admission gate concurrency limit (prevents queue buildup)
export LB_THRESHOLD="0.70"    # Dynamic scoring tolerance threshold
export LB_W_CONN="0.40"  # Connection weight in scoring equation
export LB_W_LAT="0.35"  # EWMA latency weight in scoring equation
export LB_W_CPU="0.25"     # CPU telemetry weight in scoring equation
export GOMEMLIMIT="45MiB"  #Go runtime memory ceiling to prevent OOM
export GOGC="20"     # Aggressive garbage collection trigger

# Execute the Load Balancer
./lb
```

---

### Step 5: Verifying Health & System Telemetry

Test that the entire cluster is operational through the load balancer:

```bash
# 1. Health check across backends
curl -i http://localhost:5297/health

# 2. Cluster resource telemetry and metrics
curl -s http://localhost:5297/metrics | jq .

# 3. Post a test message through the Load Balancer
curl -X POST http://localhost:5297/message \
  -H "Content-Type: application/json" \
  -d '{"client-name": "Alice", "msg": "Hello Distributed World!", "room": "general"}'

# 4. Fetch the global message feed
curl -s "http://localhost:5297/feed?room=general" | jq .
```

---

## Project Directory Structure

```
.
├── app.py                      # FastAPI application: routes, WS handler, lifecycle hooks
├── clear_db.py                 # Utility script to wipe MongoDB collections
├── requirements.txt            # Python dependencies (fastapi, uvicorn, pymongo, cryptography)
│
├── load-balancer/              # System 1: Go Edge Load Balancer
│   ├── main.go                 # Dynamic EWMA scoring, clamped sockets, zero-alloc proxy
│   └── go.mod                  # Go module definitions
│
├── server/                     # Systems 2-4: Core backend modules
│   ├── config.py               # Centralized configuration & environment loader
│   ├── crypto.py               # AES-256-GCM encryption & HMAC-SHA256 Ed25519 signing
│   ├── gossip.py               # Redis-style in-memory Pub/Sub peer gossip mesh
│   ├── logger.py               # Structured JSON logger
│   ├── rate_limiter.py         # Token bucket rate limiting definitions
│   ├── rooms.py                # In-memory room channel pub-sub subscriptions
│   ├── store.py                # Feed RAM cache, async write queue, MongoDB client
│   └── ws_server.py            # WebSocket protocol connection manager
│
├── public/                     # Static web frontend (HTML/CSS/JS)
│   ├── index.html              # Chat UI layout
│   ├── app.js                  # Client WebSocket connection & room logic
│   └── style.css               # Styling definitions
│
└── plots/                     # plots 
```

---

## License

This project is licensed under the [MIT License](LICENSE).
