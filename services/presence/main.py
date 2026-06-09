#!/usr/bin/env python3
"""
presence/main.py
================
AutoOps AI – Presence Service

Single source of truth for vehicle connectivity. Subscribes to the
Mosquitto broker's autoops/vehicles/{connected,disconnected} topics and:

  1. Maintains an in-memory presence cache for fast lookups
     (used by vehicle-telemetry, vsr, rlu before serving requests)
  2. Writes :Vehicle nodes + CONNECTED_TO / DISCONNECTED_FROM relationships
     to FalkorDB in real time — the dependency graph is self-maintaining
  3. Tracks $SYS broker metrics for observability

This service replaces the prior split between mqtt-bridge (graph writer)
and presence (cache). Having one MQTT subscriber per topic eliminates
the risk that the cache and the graph drift out of sync.

Exposed HTTP endpoints:
  GET  /health                  → service health + broker connectivity
  GET  /presence/{vehicle_id}   → online/offline state for one vehicle
  GET  /presence                → bulk online/offline counts + list
  GET  /graph/vehicles          → all :Vehicle nodes in FalkorDB
  GET  /vehicles/{vehicle_id}   → connectivity state (cache → falkordb fallback)
  GET  /metrics                 → event + graph counters since startup
  POST /fault/{inject,clear}    → fault toggles
"""

import os
import time
import logging
import threading
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from falkordb import FalkorDB
from graph_utils import update_and_cascade

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("presence")

# ── Configuration ─────────────────────────────────────────────────────────────

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "presence-service")
MQTT_HOST      = os.getenv("MQTT_HOST",      "mosquitto")
MQTT_PORT      = int(os.getenv("MQTT_PORT",  "1883"))
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

TOPIC_CONNECTED    = "autoops/vehicles/connected"
TOPIC_DISCONNECTED = "autoops/vehicles/disconnected"
TOPIC_SYS_CLIENTS  = "$SYS/broker/clients/connected"

# ── Shared state ──────────────────────────────────────────────────────────────

_db             = None
_graph          = None
_mqtt_connected = False
_FAULT_ACTIVE   = False
_metrics = {
    "connect_events":    0,
    "disconnect_events": 0,
    "graph_updates":     0,
    "graph_errors":      0,
    "broker_clients":    0,
    "presence_queries":  0,
    "started_at":        time.time(),
}

# Source-of-truth presence cache (fast lookups for telemetry/vsr/rlu).
# Format: { "VH-1001": {"state": "ONLINE", "last_updated": <epoch_ms>} }
# State values here are normalized: ONLINE / OFFLINE / UNKNOWN.
# (FalkorDB :Vehicle.state uses CONNECTED / DISCONNECTED — the legacy
#  vocabulary inherited from the original mqtt-bridge schema.)
_presence_cache: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# FalkorDB helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_graph():
    global _db, _graph
    if _graph is None:
        try:
            _db = FalkorDB(
                host=FALKORDB_HOST,
                port=FALKORDB_PORT,
                username=FALKORDB_USER,
                password=FALKORDB_PASS,
            )
            _graph = _db.select_graph(FALKORDB_GRAPH)
            logger.info("[FalkorDB] Connected to %s:%s", FALKORDB_HOST, FALKORDB_PORT)
        except Exception as exc:
            logger.warning("[FalkorDB] Not available yet: %s", exc)
            _graph = None
    return _graph


def ensure_broker_node():
    """
    Ensure the :MQTTBroker node exists in FalkorDB.
    Idempotent — safe to call multiple times (MERGE, not CREATE).
    """
    g = get_graph()
    if g is None:
        return
    try:
        g.query(
            """
            MERGE (b:MQTTBroker {name: 'mosquitto-broker'})
            ON CREATE SET
                b.host        = $host,
                b.port        = $port,
                b.status      = 'HEALTHY',
                b.criticality = 'HIGH',
                b.lastUpdated = timestamp(),
                b.message     = 'MQTT broker for vehicle connectivity'
            ON MATCH SET
                b.status      = 'HEALTHY',
                b.lastUpdated = timestamp()
            """,
            {"host": MQTT_HOST, "port": MQTT_PORT}
        )
        logger.info("[FalkorDB] MQTTBroker node ensured")
    except Exception as exc:
        logger.error("[FalkorDB] Failed to ensure broker node: %s", exc)


def on_vehicle_connected(vehicle_id: str):
    """Write CONNECTED state to both cache and FalkorDB."""
    now_ms = int(time.time() * 1000)
    _presence_cache[vehicle_id] = {"state": "ONLINE", "last_updated": now_ms}

    g = get_graph()
    if g is None:
        _metrics["graph_errors"] += 1
        logger.warning("[FalkorDB] Skipping graph update — driver unavailable")
        return

    try:
        g.query(
            """
            MERGE (v:Vehicle {name: $vid})
            ON CREATE SET
                v.state       = 'CONNECTED',
                v.firstSeen   = $ts,
                v.lastUpdated = $ts
            ON MATCH SET
                v.state       = 'CONNECTED',
                v.lastUpdated = $ts

            WITH v
            MATCH (b:MQTTBroker {name: 'mosquitto-broker'})
            OPTIONAL MATCH (v)-[old:DISCONNECTED_FROM]->(b)
            DELETE old

            WITH v, b
            MERGE (v)-[r:CONNECTED_TO]->(b)
            SET r.since       = $ts,
                r.lastUpdated = $ts
            """,
            {"vid": vehicle_id, "ts": now_ms}
        )
        _metrics["graph_updates"] += 1
        logger.debug("[Graph] %s → CONNECTED", vehicle_id)
    except Exception as exc:
        _metrics["graph_errors"] += 1
        logger.error("[FalkorDB] on_vehicle_connected failed: %s", exc)


def on_vehicle_disconnected(vehicle_id: str):
    """Write DISCONNECTED state to both cache and FalkorDB."""
    now_ms = int(time.time() * 1000)
    _presence_cache[vehicle_id] = {"state": "OFFLINE", "last_updated": now_ms}

    g = get_graph()
    if g is None:
        _metrics["graph_errors"] += 1
        logger.warning("[FalkorDB] Skipping graph update — driver unavailable")
        return

    try:
        g.query(
            """
            MERGE (v:Vehicle {name: $vid})
            SET v.state       = 'DISCONNECTED',
                v.lastUpdated = $ts

            WITH v
            MATCH (b:MQTTBroker {name: 'mosquitto-broker'})
            OPTIONAL MATCH (v)-[old:CONNECTED_TO]->(b)
            DELETE old

            WITH v, b
            MERGE (v)-[r:DISCONNECTED_FROM]->(b)
            SET r.at          = $ts,
                r.lastUpdated = $ts
            """,
            {"vid": vehicle_id, "ts": now_ms}
        )
        _metrics["graph_updates"] += 1
        logger.debug("[Graph] %s → DISCONNECTED", vehicle_id)
    except Exception as exc:
        _metrics["graph_errors"] += 1
        logger.error("[FalkorDB] on_vehicle_disconnected failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# MQTT callbacks
# ══════════════════════════════════════════════════════════════════════════════

def on_connect(client, userdata, flags, rc, properties=None):
    global _mqtt_connected
    if rc == 0:
        _mqtt_connected = True
        logger.info("[MQTT] Connected to broker %s:%s", MQTT_HOST, MQTT_PORT)
        client.subscribe(TOPIC_CONNECTED,    qos=1)
        client.subscribe(TOPIC_DISCONNECTED, qos=1)
        client.subscribe(TOPIC_SYS_CLIENTS,  qos=0)
        logger.info("[MQTT] Subscribed to %s, %s, %s",
                    TOPIC_CONNECTED, TOPIC_DISCONNECTED, TOPIC_SYS_CLIENTS)
        update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "Connected to MQTT broker")
    else:
        _mqtt_connected = False
        logger.error("[MQTT] Connection failed, return code %s", rc)
        update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", f"MQTT connect failed rc={rc}")


def on_disconnect(client, userdata, rc, properties=None):
    global _mqtt_connected
    _mqtt_connected = False
    logger.warning("[MQTT] Disconnected from broker (rc=%s) — will auto-reconnect", rc)
    update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", "Lost connection to MQTT broker")


def on_message(client, userdata, msg):
    """Route incoming MQTT messages to cache + graph handlers."""
    topic   = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace").strip()

    if topic == TOPIC_CONNECTED:
        vehicle_id = payload if payload else f"VH-UNKNOWN-{int(time.time())}"
        _metrics["connect_events"] += 1
        on_vehicle_connected(vehicle_id)

    elif topic == TOPIC_DISCONNECTED:
        vehicle_id = payload if payload else f"VH-UNKNOWN-{int(time.time())}"
        _metrics["disconnect_events"] += 1
        on_vehicle_disconnected(vehicle_id)

    elif topic == TOPIC_SYS_CLIENTS:
        try:
            _metrics["broker_clients"] = int(payload)
        except ValueError:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# MQTT client startup (runs in a background daemon thread)
# ══════════════════════════════════════════════════════════════════════════════

def start_mqtt_loop():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="autoops-presence-service",
        clean_session=True,
    )
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    while True:
        try:
            logger.info("[MQTT] Connecting to %s:%s ...", MQTT_HOST, MQTT_PORT)
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as exc:
            logger.warning("[MQTT] Broker not ready (%s) — retrying in 5 s", exc)
            time.sleep(5)

    client.loop_forever(retry_first_connection=True)


# ══════════════════════════════════════════════════════════════════════════════
# FastAPI lifespan + HTTP endpoints
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[Presence] Starting Presence service (graph-writer + cache)")
    get_graph()
    ensure_broker_node()
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "Service started")

    t = threading.Thread(target=start_mqtt_loop, daemon=True, name="mqtt-loop")
    t.start()
    logger.info("[Presence] MQTT loop thread started")

    yield

    update_and_cascade(get_graph(), SERVICE_NAME, "UNREACHABLE", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="AutoOps Presence Service", lifespan=lifespan)


@app.get("/health")
def health():
    """Health check — covers self-fault, MQTT broker connectivity, and FalkorDB."""
    global _FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("self: service fault injected")

    if not _mqtt_connected:
        issues.append("upstream:mosquitto-broker unreachable (MQTT not connected)")

    graph_ok = get_graph() is not None
    if not graph_ok:
        issues.append("FalkorDB not reachable")

    status  = "UNHEALTHY" if issues else "HEALTHY"
    message = "; ".join(issues) if issues else ""
    update_and_cascade(get_graph(), SERVICE_NAME, status, message)

    return JSONResponse(
        status_code=503 if status == "UNHEALTHY" else 200,
        content={
            "service":            SERVICE_NAME,
            "status":             status,
            "mqtt_connected":     _mqtt_connected,
            "falkordb_connected": graph_ok,
            "tracked_vehicles":   len(_presence_cache),
            "issues":             issues,
            "timestamp":          time.time(),
        }
    )


# ── Presence API (fast lookups via in-memory cache) ───────────────────────────

@app.get("/presence/{vehicle_id}")
def get_presence(vehicle_id: str):
    """
    Return online/offline state for a single vehicle from the cache.
    Called by vehicle-telemetry, vsr, and rlu before performing read or
    write operations against a vehicle.
    """
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    _metrics["presence_queries"] += 1
    entry = _presence_cache.get(vehicle_id)
    if entry is None:
        return {
            "vehicle_id": vehicle_id,
            "state":      "UNKNOWN",
            "online":     False,
            "source":     "not_seen",
        }
    return {
        "vehicle_id":   vehicle_id,
        "state":        entry["state"],
        "online":       entry["state"] == "ONLINE",
        "last_updated": entry["last_updated"],
        "source":       "cache",
    }


@app.get("/presence")
def get_all_presence():
    """Return bulk presence — counts plus a list of all tracked vehicles."""
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    online  = [v for v, s in _presence_cache.items() if s["state"] == "ONLINE"]
    offline = [v for v, s in _presence_cache.items() if s["state"] == "OFFLINE"]
    return {
        "online_count":  len(online),
        "offline_count": len(offline),
        "total_tracked": len(_presence_cache),
        "online":        online,
        "offline":       offline,
        "timestamp":     time.time(),
    }


# ── Vehicle graph queries (inherited from mqtt-bridge) ────────────────────────

@app.get("/graph/vehicles")
def graph_vehicles():
    """Return all :Vehicle nodes currently in FalkorDB with their broker edges."""
    g = get_graph()
    if g is None:
        return JSONResponse(status_code=503, content={"error": "FalkorDB unavailable"})
    try:
        result = g.query(
            """
            MATCH (v:Vehicle)
            OPTIONAL MATCH (v)-[r]->(b:MQTTBroker)
            RETURN v.name        AS vehicle_id,
                   v.state       AS state,
                   v.firstSeen   AS first_seen_ms,
                   v.lastUpdated AS last_updated_ms,
                   type(r)       AS relationship
            ORDER BY v.lastUpdated DESC
            """
        )
        vehicles = [
            {key: value for key, value in zip(result.header, row)}
            for row in result.result_set
        ]
        return {"vehicle_count": len(vehicles), "vehicles": vehicles}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/vehicles/{vehicle_id}")
def get_vehicle_status(vehicle_id: str):
    """
    Backwards-compatible endpoint inherited from mqtt-bridge.
    Returns connectivity state using the cache first, then FalkorDB as a fallback.

    State values use the legacy mqtt-bridge vocabulary (CONNECTED / DISCONNECTED)
    for compatibility with existing callers and the dashboard's vehicle panel.
    New callers should prefer /presence/{vehicle_id} which uses ONLINE/OFFLINE.
    """
    entry = _presence_cache.get(vehicle_id)
    if entry is None:
        # Not seen via MQTT yet — check FalkorDB as fallback
        g = get_graph()
        if g:
            try:
                result = g.query(
                    "MATCH (v:Vehicle {name: $vid}) RETURN v.state AS state",
                    {"vid": vehicle_id}
                )
                if result.result_set:
                    state = result.result_set[0][0]
                    # Normalize into the cache so subsequent lookups are fast.
                    normalized = "ONLINE" if state == "CONNECTED" else "OFFLINE"
                    _presence_cache[vehicle_id] = {
                        "state": normalized,
                        "last_updated": int(time.time() * 1000),
                    }
                    return {
                        "vehicle_id": vehicle_id,
                        "state":      state,
                        "online":     state == "CONNECTED",
                        "source":     "falkordb",
                    }
            except Exception:
                pass
        return {"vehicle_id": vehicle_id, "state": "UNKNOWN", "online": False, "source": "not_seen"}

    # Re-emit the legacy CONNECTED/DISCONNECTED vocabulary for this endpoint.
    legacy_state = "CONNECTED" if entry["state"] == "ONLINE" else "DISCONNECTED"
    return {
        "vehicle_id":   vehicle_id,
        "state":        legacy_state,
        "online":       entry["state"] == "ONLINE",
        "last_updated": entry["last_updated"],
        "source":       "cache",
    }


# ── Fault injection + metrics + root ──────────────────────────────────────────

@app.post("/fault/inject")
def inject_fault():
    global _FAULT_ACTIVE
    _FAULT_ACTIVE = True
    update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", "Service fault injected via API")
    logger.warning("[Presence] Fault injected")
    return {"message": f"Fault injected into {SERVICE_NAME}", "status": "UNHEALTHY"}


@app.post("/fault/clear")
def clear_fault():
    global _FAULT_ACTIVE
    _FAULT_ACTIVE = False
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "")
    logger.info("[Presence] Fault cleared")
    return {"message": f"Fault cleared on {SERVICE_NAME}", "status": "HEALTHY"}


@app.get("/fault/status")
def fault_status():
    return {"service": SERVICE_NAME, "fault_active": _FAULT_ACTIVE}


@app.get("/metrics")
def metrics():
    uptime = time.time() - _metrics["started_at"]
    return {
        **_metrics,
        "tracked_vehicles": len(_presence_cache),
        "uptime_seconds":   round(uptime, 1),
    }


@app.get("/")
def root():
    return {
        "service":     SERVICE_NAME,
        "version":     "2.0.0",
        "description": "Vehicle online/offline presence + :Vehicle graph maintenance (merged from mqtt-bridge)",
    }
