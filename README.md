# AutoOps AI Agent — PoC

A local Proof of Concept demonstrating an autonomous AutoOps AI agent that
detects incidents, performs graph-aware root cause analysis (RCA), and
self-heals a microservices-based automotive platform using **FalkorDB** as the
dependency graph store.

The PoC also proves the **self-maintaining graph concept** using a live MQTT
data stream: vehicle connect/disconnect events automatically update the FalkorDB
dependency graph in real time — zero manual overhead.

The graph also stores **operations history** — past Jira incident tickets linked
to the services and remediations that resolved them. The AI agent uses this
history during RCA to identify recurring failures and apply proven fixes with
measurable confidence.

The PoC also demonstrates **log-based RCA** — when the dependency graph cannot
explain a failure (all services appear healthy), the agent reads live service
logs via `docker logs` to identify unknown or intermittent failure patterns.

---

## Architecture

```
Vehicles (simulated)
      │  MQTT PUBLISH (connect / disconnect events)
      ▼
mosquitto-broker           (:1883)   ← MQTT broker for vehicle connectivity
      │  paho-mqtt subscribe
      ▼
presence-service           (:8007)   ← owns vehicle online/offline state
                                       AND maintains :Vehicle graph in FalkorDB
      ▲  HTTP (presence checks)
      │
      ├── vehicle-telemetry-service  (:8001)   ← checks vehicle online status before serving telemetry
      │       │ HTTP
      │       ├──> diagnostics-service        (:8002)   ← depends on telemetry + SQLite
      │       │       │ HTTP          │ SQLite
      │       │       ▼               ▼
      │       │     maintenance-service (:8003)   sqlite-dtc-db
      │       │       │ HTTP
      │       │       ▼
      │       │     notification-service (:8004)
      │       │       │ HTTP            │ async publish (optional)
      │       │       ▼                 ▼
      │       │     dealer-portal       mnp-service (:8010)
      │       │     (:8005)             ← pub/sub broker; async fan-out
      │       │
      │       └──> vsr-service (:8008)   ← Vehicle Status Report
      │                                   (parallel read path off telemetry)
      │
      └── rlu-service             (:8009)   ← Remote Lock/Unlock (write/command path)
              │  MQTT PUBLISH (commands → vehicles)
              ▼
            mosquitto-broker

FalkorDB  (:6379 redis, :3000 browser)   ← dependency graph + state store
demo-dashboard (:8090)                   ← live PoC dashboard
AI Agent                                 ← reads graph, polls health, remediates
```

### Dependency graph in FalkorDB

```
# Read path (original chain)
vehicle-telemetry-service  -[DEPENDS_ON:http,required]->   presence-service
diagnostics-service        -[DEPENDS_ON:http,required]->   vehicle-telemetry-service
diagnostics-service        -[DEPENDS_ON:sqlite,required]-> sqlite-dtc-db
maintenance-service        -[DEPENDS_ON:http,required]->   diagnostics-service
notification-service       -[DEPENDS_ON:http,required]->   maintenance-service
dealer-portal-service      -[DEPENDS_ON:http,required]->   notification-service

# Parallel read path
vsr-service                -[DEPENDS_ON:http,required]->   vehicle-telemetry-service
vsr-service                -[DEPENDS_ON:http,required]->   presence-service

# Write/command path
rlu-service                -[DEPENDS_ON:http,required]->   presence-service
rlu-service                -[DEPENDS_ON:mqtt,required]->   mosquitto-broker

# Broker subscribers
presence-service           -[DEPENDS_ON:mqtt,required]->   mosquitto-broker

# Async fan-out (optional — outage does not cascade)
notification-service       -[DEPENDS_ON:http,optional]->   mnp-service

# Vehicle fleet (maintained by presence-service in real time)
(:Vehicle)-[:CONNECTED_TO / :DISCONNECTED_FROM]->(:MQTTBroker)

# Operations history
(:Service / :Datastore)-[:HAS_REMEDIATION]->(:Remediation)
(:JiraTicket)-[:AFFECTED]->(:Service / :Datastore)
(:JiraTicket)-[:RESOLVED_BY]->(:Remediation)
```

Edge property `required` distinguishes hard dependencies (default `true` —
failure cascades upstream) from optional ones (false — failure does not
cascade; notification → mnp is the only optional edge today).

---

## Prerequisites

- Docker Desktop (with Compose v2)
- Python 3.9+
- pip

---

## Quick Start

### 1. Start all containers

```bash
docker compose up -d --build
```

Wait ~30 seconds for FalkorDB and mosquitto to fully initialise. All 12
containers should be running: `falkordb`, `mosquitto`, `presence`,
`vehicle-telemetry`, `diagnostics`, `maintenance`, `notification`,
`dealer-portal`, `vsr`, `rlu`, `mnp`, `demo-dashboard`.

### 2. Install Python dependencies for scripts

```bash
pip install falkordb==1.6.1 httpx==0.27.0 paho-mqtt==2.1.0 openpyxl fastmcp
```

### 3. Seed the FalkorDB graph

The seed script supports three modes depending on your use case:

```bash
# Standard development / full seed (includes Jira ticket history)
python scripts/seed_falkordb.py

# Demo mode — seed WITHOUT Jira history (use before Use Case 1 and 2)
python scripts/seed_falkordb.py --no-tickets

# Add Jira tickets to an already-seeded graph (use during Use Case 3 transition)
python scripts/seed_falkordb.py --tickets-only
```

For demo runs always start with `--no-tickets`. Jira history is added live
during Use Case 3 using `--tickets-only`.

`jira_tickets.xlsx` must be present in the project root.

Expected output (full seed):
```
  Datastore: 1 node(s)
  JiraTicket: 8 node(s)
  MQTTBroker: 1 node(s)
  Remediation: 3 node(s)
  Service: 9 node(s)
  DEPENDS_ON relationships: 13
  HAS_REMEDIATION relationships: 19
  AFFECTED edges: 8
  RESOLVED_BY edges: 8
```

Expected output (`--no-tickets`):
```
  Datastore: 1 node(s)
  MQTTBroker: 1 node(s)
  Remediation: 3 node(s)
  Service: 9 node(s)
  DEPENDS_ON relationships: 13
  HAS_REMEDIATION relationships: 19
```

> Counts assume the current `seed_falkordb.py` topology. If your seed script
> still uses the old 6-service / 8-edge layout, expected numbers will be lower.

### 4. Verify all services are healthy

```bash
python scripts/verify_health.py
```

Expected output: all 9 services showing ✅ HEALTHY.

### 5. Verify MQTT integration

```bash
python scripts/verify_mqtt.py
```

Runs an end-to-end check: broker reachability → presence-service health →
FalkorDB :Vehicle nodes → live publish/read round-trip. All checks should pass.

### 6. Open FalkorDB Browser (optional)

Navigate to http://localhost:3000

Fill in the connection form:
- Host: `localhost`
- Port: `6379`
- Username: `sampleuser`
- Password: `samplePass123`
- TLS: OFF

Run `MATCH (n) RETURN n` to visualise the full dependency graph.

### 7. Open the PoC Dashboard

The bundled `demo-dashboard` service exposes a live UI at:

```
http://localhost:8090
```

It renders all 9 service cards, the live FalkorDB graph, container states,
live logs, and the demo scenario buttons. The dashboard uses relative URLs,
so the same HTML works locally and in cloud deployments.

---

## Demo Use Cases

The PoC demonstrates four use cases that build on each other in sequence.
For full step-by-step demo execution including AI agent interaction, see
`AutoOps_AI_Demo_Run_Guide.docx`.

---

### Use Case 1 — Mass Disconnect Triage

Demonstrates the agent distinguishing a platform failure from an external
network event using the dependency graph.

**Part A — External event (false alarm suppression):**
```bash
# Connect fleet
python scripts/simulate_vehicles.py --scenario rush_hour

# Run wave — disconnect half the fleet with NO platform fault injected
python scripts/simulate_vehicles.py --scenario wave
```
Agent checks presence-service and vehicle-telemetry health. Both HEALTHY →
concludes external event, suppresses alert. No remediation needed.

**Part B — Platform failure:**
```bash
# Connect fleet, then inject a platform fault into presence-service
python scripts/simulate_vehicles.py --scenario rush_hour
curl -X POST http://localhost:8007/fault/inject
```
Agent finds presence-service UNHEALTHY, traverses graph, identifies it as the
root cause, and remediates via `clear_service_fault("presence-service")`.

> Note: presence-service now owns both the broker subscription and the
> `:Vehicle` graph (the former `mqtt-bridge-service` was merged into it).
> A fault on presence has a richer blast radius than the old mqtt-bridge fault —
> it breaks the read path (telemetry → diagnostics → … → dealer-portal),
> the parallel read path (vsr), and the write path (rlu).

**Agent prompt (both Part A and Part B):**
```
We are seeing a significant number of vehicles going offline.
Investigate the platform and advise whether this requires any action.
```

**Approval message (Part B only):**
```
Approved. Proceed with remediation.
```

**Reset after Use Case 1:**
```bash
curl -X POST http://localhost:8007/fault/clear
# FalkorDB Browser: MATCH (v:Vehicle) DETACH DELETE v
```

---

### Use Case 2 — Graph-Aware RCA and Self-Healing

Demonstrates the agent identifying a datastore failure hidden behind a
cascade of service failures. Run with `--no-tickets` seed (no Jira history).

```bash
python scripts/simulate_failure.py 2
```

Injects a fault into `sqlite-dtc-db` inside the diagnostics container.
The container keeps running but DTC lookups fail. Downstream services
cascade to UNHEALTHY.

Agent uses graph traversal to find `sqlite-dtc-db` (Datastore node, not a
Service) as the root cause — and applies REM-003 (clear DB fault flag) rather
than restarting the container.

**Agent prompt:**
```
A fault has been injected into the automotive platform. Investigate
the system, identify the root cause, and produce a full incident
report with your recommended remediation plan. Wait for my approval
before executing any fixes.
```

**Approval message:**
```
Approved. Proceed with remediation.
```

**Reset after Use Case 2:**
```bash
python scripts/simulate_failure.py clear
# Do NOT wipe the graph — Use Case 3 continues on the same graph
```

---

### Use Case 3 — Operations History and Confidence-Driven Remediation

Demonstrates the agent recognising a recurring failure from Jira ticket
history. Run immediately after Use Case 2 on the same healthy graph.

**Add ticket history to the live graph (mid-demo transition):**
```bash
python scripts/seed_falkordb.py --tickets-only
```

Show the audience in FalkorDB Browser:
```cypher
MATCH (t:JiraTicket)-[:AFFECTED]->(n {name:'sqlite-dtc-db'})
RETURN t.name, t.occurred_at, t.summary ORDER BY t.occurred_at
```

Then inject the same fault as Use Case 2:
```bash
python scripts/simulate_failure.py 2
```

This time the agent finds 3 past tickets for `sqlite-dtc-db` (INC-001,
INC-004, INC-007), all resolved by REM-003. It reports 100% confidence,
skips investigation, and flags the node as a chronic issue.

**Agent prompt (same as Use Case 2):**
```
A fault has been injected into the automotive platform. Investigate
the system, identify the root cause, and produce a full incident
report with your recommended remediation plan. Wait for my approval
before executing any fixes.
```

**Approval message:**
```
Approved. Proceed with remediation.
```

**Reset after Use Case 3:**
```bash
python scripts/simulate_failure.py clear
```

---

### Use Case 4 — Log Analysis Fallback

Demonstrates the agent falling back to log analysis when the dependency graph
is inconclusive. All services appear HEALTHY in FalkorDB, but `diagnostics-service`
is generating intermittent DTC database errors that only appear in `docker logs`.

No graph signal. No Jira history. No `HAS_REMEDIATION` edge for this failure mode.
The agent must discover the root cause entirely from log evidence.

```bash
python scripts/simulate_failure.py 5
```

Starts a background thread inside `diagnostics-service` that logs real DTC
database lookup errors every 3 seconds — without setting any fault flag or
updating FalkorDB state.

Verify the graph stays healthy while errors accumulate in logs:
```bash
# Graph — all HEALTHY
python scripts/verify_health.py

# Logs — ERROR entries visible
docker logs diagnostics --tail 30
```

**Agent prompt:**
```
The engineering team has received user complaints about intermittent
failures on the automotive platform over the past hour. Internal
monitoring shows all services as healthy. No alerts have fired.
Investigate and report your findings.
```

**Reset after Use Case 4:**
```bash
python scripts/simulate_failure.py clear
```

---

### Other fault injection options

```bash
python scripts/simulate_failure.py 1      # Root service failure (full cascade)
python scripts/simulate_failure.py 3      # Mid-chain failure
python scripts/simulate_failure.py status # Show current fault flags
python scripts/simulate_failure.py clear  # Clear all injected faults
python scripts/verify_health.py --watch   # Watch mode
```

---

## Telemetry + MQTT Integration

`vehicle-telemetry-service` checks vehicle connectivity via `presence-service`
before serving telemetry data.

```bash
# Connected vehicle — returns full telemetry
curl http://localhost:8001/telemetry/VH-1001
# → { "connection_state": "CONNECTED", "speed_kmh": 87, ... }

# Disconnected vehicle — returns offline response
curl http://localhost:8001/telemetry/VH-1001
# → { "status": "OFFLINE", "connection_state": "DISCONNECTED", "message": "..." }
```

`vsr-service` aggregates raw telemetry into user-friendly business data:

```bash
curl http://localhost:8008/status/VH-1001
# → { "vehicle_id": "VH-1001", "online": true,
#     "fuel": {"level_pct": 67, "status": "OK"},
#     "battery": {"voltage": 12.6, "status": "OK"}, ... }
```

`rlu-service` issues lock/unlock commands over MQTT (the write path):

```bash
# Lock a vehicle (checks presence first, then publishes via MQTT)
curl -X POST http://localhost:8009/command/VH-1001/lock
# → { "command_id": "CMD-...", "status": "ISSUED" }

# Returns 409 if vehicle is offline; 502 if MQTT publish fails;
# 503 if presence-service is unreachable.

curl http://localhost:8009/command/CMD-xxxxxxxx
# → { "status": "ISSUED", "vehicle_id": "VH-1001", "action": "LOCK", ... }
```

---

## Service API Reference

Every microservice exposes these endpoints:

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check — polls upstream, updates FalkorDB state |
| `/fault/inject` | POST | Inject a service-level fault |
| `/fault/clear` | POST | Clear all faults |
| `/fault/status` | GET | Show current fault flags |

### Service-specific endpoints

| Service | Endpoint | Description |
|---|---|---|
| vehicle-telemetry | `GET /telemetry` | Random vehicle telemetry |
| vehicle-telemetry | `GET /telemetry/{vehicle_id}` | Telemetry for a specific vehicle — checks presence-service first |
| diagnostics | `GET /diagnostics/{vehicle_id}` | DTCs + telemetry for vehicle |
| diagnostics | `GET /dtc-codes` | All DTC codes in SQLite |
| diagnostics | `POST /fault/inject-db` | Inject SQLite DB fault only |
| diagnostics | `POST /fault/inject-silent` | Inject silent fault — logs errors, graph stays HEALTHY (Use Case 4) |
| diagnostics | `POST /fault/clear-silent` | Stop the silent fault log thread |
| maintenance | `GET /maintenance/{vehicle_id}` | Maintenance schedule |
| notification | `POST /notify/{vehicle_id}` | Send notification (also best-effort publishes to MNP) |
| notification | `GET /notifications` | View notification log |
| dealer-portal | `GET /dealer/dashboard/{dealer_id}` | Dealer dashboard |
| dealer-portal | `GET /dealer/vehicles` | Fleet vehicle list |
| presence | `GET /presence/{vehicle_id}` | Online/offline state for one vehicle |
| presence | `GET /presence` | Bulk presence — counts + lists |
| presence | `GET /graph/vehicles` | All :Vehicle nodes from FalkorDB |
| presence | `GET /vehicles/{vehicle_id}` | Legacy connection-state endpoint (CONNECTED / DISCONNECTED) |
| presence | `GET /metrics` | Event counters and graph-update counters |
| vsr | `GET /status/{vehicle_id}` | User-friendly vehicle status report |
| rlu | `POST /command/{vehicle_id}/lock` | Issue LOCK command (returns CMD-* id) |
| rlu | `POST /command/{vehicle_id}/unlock` | Issue UNLOCK command |
| rlu | `GET /command/{command_id}` | Command status (ISSUED / FAILED / ACKED / TIMEOUT) |
| rlu | `GET /commands` | Recent commands (default 50) |
| mnp | `POST /publish/{topic}` | Publish a message to a topic |
| mnp | `POST /subscribe/{topic}` | Register a subscriber, returns subscriber_id |
| mnp | `GET /consume/{topic}/{subscriber_id}` | Pull next message from queue |
| mnp | `GET /topics` | List all topics with subscriber count + queue depth |
| mnp | `GET /metrics` | Pub/sub broker metrics |

---

## FalkorDB Graph Queries

Open FalkorDB Browser at http://localhost:3000 (connect as `sampleuser` / `samplePass123`)
and run these in the `autoops` graph:

```cypher
-- View full graph
MATCH (n) RETURN n

-- All service states
MATCH (s:Service) RETURN s.name, s.state, s.criticality, s.message

-- Dependency chain (note the `required` flag — false = soft dep)
MATCH (a)-[r:DEPENDS_ON]->(b)
RETURN a.name, r.type, r.required, b.name

-- Find root cause candidates (unhealthy, no unhealthy required deps)
MATCH (n) WHERE (n:Service OR n:Datastore) AND n.state = 'UNHEALTHY'
AND NOT EXISTS {
  MATCH (n)-[r:DEPENDS_ON]->(dep)
  WHERE dep.state = 'UNHEALTHY' AND r.required = true
}
RETURN n.name, labels(n)[0] AS type, n.message

-- All unhealthy nodes
MATCH (n) WHERE (n:Service OR n:Datastore) AND n.state = 'UNHEALTHY'
RETURN n.name, labels(n)[0] AS type, n.message

-- Downstream impact from a given root cause
MATCH path = (root {name: 'presence-service'})<-[:DEPENDS_ON*1..]-(affected:Service)
RETURN affected.name, length(path) AS hops ORDER BY hops

-- Live vehicle fleet — connected and disconnected
MATCH (v:Vehicle)-[r]->(b:MQTTBroker)
RETURN v.name, v.state, type(r), b.name
ORDER BY v.state

-- Count connected vs disconnected vehicles
MATCH (v:Vehicle)
RETURN v.state, count(v) AS count
```

---

## AI Agent Setup

### 1. Start the MCP server and Cloudflare Tunnel

```bash
chmod +x scripts/start_mcp.sh
./scripts/start_mcp.sh
```

This starts the MCP server on port 8080 and exposes it via a Cloudflare Tunnel.
The public URL is printed at the end and saved to `agent_inputs/mcp_server_url.txt`.

### 2. Configure the AI agent

Open your AI agent platform and configure:

- **Agent instructions:** paste the contents of `agent_qa.txt` — the platform
  generates system instructions from these Q&A answers
- **MCP Server URL:** the `https://xxxx.trycloudflare.com/mcp` URL from Step 1
- **Transport:** `streamable-http`

### MCP tools available to the agent

| Category | Tool | Purpose |
|---|---|---|
| Snapshot | `collect_full_system_snapshot` | Complete system state in one call |
| Health | `check_all_services_health` | Poll all 9 /health endpoints |
| Health | `check_service_health` | Poll one service /health |
| Graph | `get_graph_state` | Full FalkorDB node + relationship snapshot |
| Graph | `find_root_cause_candidates` | RCA Cypher query result |
| Graph | `get_downstream_impact` | Services affected by a root cause |
| Graph | `get_remediation_actions` | Fetch REM-001/002/003 from FalkorDB |
| Vehicle Graph | `get_vehicle_graph_state` | Full vehicle fleet state from FalkorDB |
| Vehicle Graph | `get_connected_vehicles` | Currently connected vehicles |
| Vehicle Graph | `get_disconnected_vehicles` | Vehicles currently offline |
| Vehicle Graph | `get_presence_service_metrics` | Event counters and presence-service uptime |
| Presence | `get_presence` | Online/offline state for one vehicle |
| Presence | `get_presence_summary` | Bulk presence — counts + lists |
| VSR | `get_vehicle_status_report` | User-friendly vehicle status for a vehicle |
| RLU (write) | `send_lock_command` | Issue LOCK / UNLOCK command via MQTT |
| RLU (write) | `get_command_status` | Look up a previously issued command |
| RLU (write) | `list_recent_commands` | Recent commands issued |
| MNP | `get_mnp_topics` | List topics with subscriber count + queue depth |
| MNP | `get_mnp_metrics` | Pub/sub broker metrics |
| History | `get_historical_incidents` | Past tickets for a root cause node |
| History | `get_all_jira_tickets` | Full incident history |
| History | `get_recurring_failures` | Nodes appearing as root cause 2+ times |
| Remediation | `clear_service_fault` | REM-002 / REM-003 |
| Remediation | `restart_container` | REM-001 for stopped containers |
| Remediation | `wait_for_stabilisation` | Pause after remediation before verifying |
| Log Analysis | `get_service_logs` | Recent logs from a single service via docker logs |
| Log Analysis | `get_all_service_logs` | Logs from all containers in one call — per-service error summary |

---

## Project Structure

```
autoops-poc/
├── docker-compose.yml                     # 12 services (FalkorDB + Mosquitto + 9 micro + dashboard)
├── README.md
├── jira_tickets.xlsx                      # Dummy Jira incident history (read by seed_falkordb.py)
├── agent_qa.txt                           # Agent Q&A — paste into agent creator platform
├── cloudflare/
│   └── config.yml                         # Cloudflare Tunnel config for MCP server
├── mosquitto/
│   └── mosquitto.conf                     # Mosquitto broker config
├── services/
│   ├── vehicle-telemetry/                 # Checks presence before serving telemetry
│   ├── diagnostics/                       # SQLite + fault inject/inject-silent endpoints
│   ├── maintenance/
│   ├── notification/                      # HTTP path + best-effort async publish to MNP
│   ├── dealer-portal/
│   ├── presence/                          # MQTT subscriber + :Vehicle graph writer
│   │                                      # (merged from former mqtt-bridge service)
│   ├── vsr/                               # Vehicle Status Report (telemetry + presence aggregator)
│   ├── rlu/                               # Remote Lock/Unlock (MQTT publisher, write path)
│   ├── mnp/                               # Messaging & Notification Platform (pub/sub broker)
│   └── demo-dashboard/                    # FastAPI + HTML demo UI (port 8090)
├── mcp_server/
│   ├── server.py                          # MCP tools — health, graph, vehicle, presence, vsr, rlu, mnp
│   └── requirements.txt
├── agent_inputs/
│   └── mcp_server_url.txt                 # Auto-written by start_mcp.sh
└── scripts/
    ├── seed_falkordb.py                   # Seeds graph; --no-tickets and --tickets-only flags
    ├── start_mcp.sh                       # Starts MCP server + Cloudflare Tunnel
    ├── simulate_vehicles.py               # MQTT vehicle event simulator
    ├── verify_mqtt.py                     # End-to-end MQTT pipeline health check
    ├── simulate_failure.py                # Injects/clears service faults
    └── verify_health.py                   # Health report + FalkorDB state
```

---

## Stopping the PoC

```bash
# Stop all containers
docker compose down

# Stop and remove all data volumes (full reset)
docker compose down -v
```

---

## FalkorDB Setup

Once you run `docker compose up`, FalkorDB exposes its browser UI on port
3000. Open http://localhost:3000 and connect with:

- Host: `localhost`
- Port: `6379`
- Username: `sampleuser`
- Password: `samplePass123`
- TLS: OFF

FalkorDB uses Redis wire protocol on port 6379 and speaks Cypher — most
queries from the old Neo4j PoC port over unchanged.

---

## Troubleshooting

**FalkorDB takes too long to start:**
The healthcheck pings on Redis port 6379. If startup fails,
run `docker compose logs falkordb` to inspect.

**Services show UNREACHABLE:**
Ensure Docker is running and ports 8001–8010, 1883, 6379, and 3000 are not in use.
Run `docker compose ps` to check container status.

**Seed script fails with connection error:**
Wait a few more seconds for FalkorDB to be ready, then re-run `seed_falkordb.py`.
The script will retry automatically.

**Services still unhealthy after clear:**
Each service polls its upstream on every `/health` request. After clearing,
simply call `/health` on any service to trigger a fresh check.

**presence-service shows UNHEALTHY:**
```bash
docker logs presence
# Look for "Broker not ready" — mosquitto may still be starting
docker compose restart presence
```

**Vehicle nodes not appearing after simulate:**
```bash
# Check presence-service processed the events
curl http://localhost:8007/metrics
# graph_updates should be > 0
# If graph_errors > 0, check: docker logs presence
```

**RLU lock/unlock returns 503:**
The command path requires both mosquitto-broker and presence-service to be
healthy. Check both:
```bash
curl http://localhost:8007/health
docker compose ps mosquitto
```

**RLU lock/unlock returns 409:**
The target vehicle is offline. Connect it first via the simulator, or pick
a vehicle from `curl http://localhost:8007/presence` (the `online` list).

**MNP publish returns 503 but notification still succeeds:**
This is expected. The `notification → mnp` edge is marked `required: false` in
the graph. Notification logs the failure (`mnp_publish_status` in its response)
but does not propagate the failure to its callers.

**Jira tickets not appearing after seed:**
Ensure `jira_tickets.xlsx` is in the project root (same directory as
`docker-compose.yml`) before running `python scripts/seed_falkordb.py`.
The script prints a warning if the file is not found rather than failing.

**openpyxl not installed:**
```bash
pip install openpyxl
```

**Agent cannot find MCP tools after restarting tunnel:**
The Cloudflare quick tunnel URL changes on every `start_mcp.sh` run. Update
the MCP Server URL in your agent platform with the new URL from
`agent_inputs/mcp_server_url.txt`.

**presence-service fault inject/clear not working:**
```bash
curl http://localhost:8007/fault/status
# If connection refused, rebuild: docker compose up -d --build presence
```

**Use Case 3 tickets not visible after --tickets-only:**
```bash
# In FalkorDB Browser: MATCH (t:JiraTicket) RETURN count(t)
# Should return 8. If 0, check jira_tickets.xlsx is in project root.
```

**Use Case 4 — no errors visible in docker logs after scenario 5:**
The silent fault auto-stops after 20 cycles (~60 seconds). Run the scenario
and trigger the agent investigation within that window. If it has already
stopped, re-run:
```bash
python scripts/simulate_failure.py 5
```

**Use Case 4 — agent finds root cause from graph instead of logs:**
Ensure you are running with `--no-tickets` seed and that no previous fault
flags are active before injecting scenario 5:
```bash
python scripts/simulate_failure.py clear
python scripts/simulate_failure.py 5
```

**Dashboard shows wrong service names:**
The dashboard renders display names via the `SERVICE_DISPLAY_NAMES` map in
`services/demo-dashboard/dashboard.html`. The underlying service identifiers
(`vsr-service`, `rlu-service`, etc.) are wire-level and must not change —
only edit the display labels.
