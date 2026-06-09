#!/usr/bin/env python3
"""
rlu/main.py
===========
AutoOps AI – Remote Lock/Unlock (RLU)

The first WRITE/COMMAND path in the AutoOps platform. Accepts
lock/unlock commands from user apps and the dealer portal, checks
vehicle presence, then publishes the command to the Mosquitto broker
which the vehicle subscribes to.

Asymmetry vs. read services:
  - When mosquitto-broker is degraded, READS still work on cached data,
    but RLU command publishes FAIL — the command path is broken even
    when the read path looks healthy.
  - When presence-service is down, RLU cannot determine whether the
    vehicle is online to receive the command — RLU degrades.

Command lifecycle:
  ISSUED  → command sent over MQTT
  ACKED   → vehicle acknowledged (out-of-scope for PoC — simulated)
  TIMEOUT → no ack within window
  FAILED  → vehicle offline / mqtt publish error

Exposed HTTP endpoints:
  GET  /health                          → service health
  POST /command/{vehicle_id}/lock       → issue lock command
  POST /command/{vehicle_id}/unlock     → issue unlock command
  GET  /command/{command_id}            → check command status
  GET  /commands                        → list recent commands
"""

import os
import time
import uuid
import json
import logging
import threading
from contextlib import asynccontextmanager

import httpx
import paho.mqtt.client as mqtt
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from falkordb import FalkorDB
from graph_utils import update_and_cascade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("rlu")

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "rlu-service")
PRESENCE_URL   = os.getenv("PRESENCE_URL",   "http://localhost:8007")
MQTT_HOST      = os.getenv("MQTT_HOST",      "mosquitto")
MQTT_PORT      = int(os.getenv("MQTT_PORT",  "1883"))
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

COMMAND_TOPIC_TEMPLATE = "autoops/vehicles/{vehicle_id}/commands"

_db = None
_graph = None
_FAULT_ACTIVE = False

_mqtt_client    = None
_mqtt_connected = False
_mqtt_lock      = threading.Lock()

# In-memory command log: { command_id: {vehicle_id, action, status, ...} }
_command_log: dict = {}
_command_lock      = threading.Lock()


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
            logger.info(f"[{SERVICE_NAME}] Connected to FalkorDB")
        except Exception as e:
            logger.warning(f"[{SERVICE_NAME}] FalkorDB not available: {e}")
            _graph = None
    return _graph


# ══════════════════════════════════════════════════════════════════════════════
# MQTT publisher client
# ══════════════════════════════════════════════════════════════════════════════

def _on_mqtt_connect(client, userdata, flags, rc, properties=None):
    global _mqtt_connected
    if rc == 0:
        _mqtt_connected = True
        logger.info("[MQTT] Publisher connected to broker %s:%s", MQTT_HOST, MQTT_PORT)
    else:
        _mqtt_connected = False
        logger.error("[MQTT] Publisher connection failed rc=%s", rc)


def _on_mqtt_disconnect(client, userdata, rc, properties=None):
    global _mqtt_connected
    _mqtt_connected = False
    logger.warning("[MQTT] Publisher disconnected (rc=%s) — will auto-reconnect", rc)


def start_mqtt_publisher():
    global _mqtt_client
    _mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="autoops-rlu-publisher",
        clean_session=True,
    )
    _mqtt_client.on_connect    = _on_mqtt_connect
    _mqtt_client.on_disconnect = _on_mqtt_disconnect

    while True:
        try:
            logger.info("[MQTT] Publisher connecting to %s:%s ...", MQTT_HOST, MQTT_PORT)
            _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as exc:
            logger.warning("[MQTT] Broker not ready (%s) — retrying in 5 s", exc)
            time.sleep(5)

    _mqtt_client.loop_forever(retry_first_connection=True)


def publish_command(vehicle_id: str, command_payload: dict) -> tuple[bool, str]:
    """
    Publish a command to the vehicle's MQTT command topic.
    Returns (success, error_message).
    """
    if not _mqtt_connected or _mqtt_client is None:
        return False, "MQTT broker not connected — command path unavailable"

    topic   = COMMAND_TOPIC_TEMPLATE.format(vehicle_id=vehicle_id)
    payload = json.dumps(command_payload)

    try:
        with _mqtt_lock:
            info = _mqtt_client.publish(topic, payload, qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return False, f"MQTT publish failed rc={info.rc}"
        return True, ""
    except Exception as exc:
        return False, f"MQTT publish exception: {exc}"


# ══════════════════════════════════════════════════════════════════════════════
# Presence check
# ══════════════════════════════════════════════════════════════════════════════

def check_vehicle_online(vehicle_id: str) -> tuple[bool, str]:
    """
    Query presence-service. Returns (online, error_message).
    A presence-service outage is treated as a failure to issue the command.
    """
    try:
        r = httpx.get(f"{PRESENCE_URL}/presence/{vehicle_id}", timeout=3.0)
        if r.status_code != 200:
            return False, f"presence-service returned {r.status_code}"
        data = r.json()
        return bool(data.get("online")), ""
    except httpx.TimeoutException:
        return False, "presence-service timed out"
    except Exception as e:
        return False, f"presence-service unreachable ({type(e).__name__})"


# ══════════════════════════════════════════════════════════════════════════════
# FastAPI lifespan + endpoints
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"[{SERVICE_NAME}] Starting RLU service")
    get_graph()
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "Service started")

    t = threading.Thread(target=start_mqtt_publisher, daemon=True, name="mqtt-publisher")
    t.start()
    logger.info(f"[{SERVICE_NAME}] MQTT publisher thread started")

    yield

    update_and_cascade(get_graph(), SERVICE_NAME, "UNREACHABLE", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="AutoOps Remote Lock/Unlock", lifespan=lifespan)


@app.get("/health")
def health():
    """
    Health covers: self-fault, MQTT broker connectivity (write path),
    and presence-service reachability (read path for online checks).
    """
    global _FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("self: service fault injected")

    if not _mqtt_connected:
        issues.append("upstream:mosquitto-broker unreachable (MQTT publisher not connected)")

    try:
        r = httpx.get(f"{PRESENCE_URL}/health", timeout=3.0)
        presence_data = r.json()
        if presence_data.get("status") != "HEALTHY":
            issues.append(
                f"upstream:presence-service is {presence_data.get('status', 'UNKNOWN')}"
            )
    except httpx.TimeoutException:
        issues.append("upstream:presence-service timed out")
    except Exception as e:
        issues.append(f"upstream:presence-service unreachable ({type(e).__name__})")

    status  = "UNHEALTHY" if issues else "HEALTHY"
    message = "; ".join(issues) if issues else ""
    update_and_cascade(get_graph(), SERVICE_NAME, status, message)

    return JSONResponse(
        status_code=503 if status == "UNHEALTHY" else 200,
        content={
            "service":        SERVICE_NAME,
            "status":         status,
            "mqtt_connected": _mqtt_connected,
            "issues":         issues,
            "timestamp":      time.time(),
        }
    )


def _issue_command(vehicle_id: str, action: str):
    """Shared logic for /command/{vid}/lock and /command/{vid}/unlock."""
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    command_id = f"CMD-{uuid.uuid4().hex[:12]}"
    now        = time.time()

    record = {
        "command_id": command_id,
        "vehicle_id": vehicle_id,
        "action":     action,
        "status":     "ISSUED",
        "issued_at":  now,
        "completed_at": None,
        "error":      None,
    }

    # 1) Presence check
    online, err = check_vehicle_online(vehicle_id)
    if err:
        record["status"] = "FAILED"
        record["error"]  = f"presence check failed: {err}"
        record["completed_at"] = time.time()
        with _command_lock:
            _command_log[command_id] = record
        logger.warning(f"[{SERVICE_NAME}] {command_id} FAILED — {record['error']}")
        return JSONResponse(status_code=503, content=record)

    if not online:
        record["status"] = "FAILED"
        record["error"]  = f"vehicle {vehicle_id} is offline — command not sent"
        record["completed_at"] = time.time()
        with _command_lock:
            _command_log[command_id] = record
        logger.info(f"[{SERVICE_NAME}] {command_id} FAILED — vehicle offline")
        return JSONResponse(status_code=409, content=record)

    # 2) Publish over MQTT
    payload = {
        "command_id": command_id,
        "action":     action,
        "issued_at":  now,
    }
    success, err = publish_command(vehicle_id, payload)
    if not success:
        record["status"] = "FAILED"
        record["error"]  = err
        record["completed_at"] = time.time()
        with _command_lock:
            _command_log[command_id] = record
        logger.error(f"[{SERVICE_NAME}] {command_id} FAILED — {err}")
        return JSONResponse(status_code=502, content=record)

    # Success — command is on the wire. Ack handling is out-of-scope for PoC.
    with _command_lock:
        _command_log[command_id] = record

    logger.info(f"[{SERVICE_NAME}] {command_id} ISSUED — {action} {vehicle_id}")
    return record


@app.post("/command/{vehicle_id}/lock")
def lock_vehicle(vehicle_id: str):
    """Send a LOCK command to the vehicle."""
    return _issue_command(vehicle_id, "LOCK")


@app.post("/command/{vehicle_id}/unlock")
def unlock_vehicle(vehicle_id: str):
    """Send an UNLOCK command to the vehicle."""
    return _issue_command(vehicle_id, "UNLOCK")


@app.get("/command/{command_id}")
def get_command_status(command_id: str):
    """Look up the status of a previously issued command."""
    with _command_lock:
        record = _command_log.get(command_id)
    if record is None:
        return JSONResponse(
            status_code=404,
            content={"error": f"Command '{command_id}' not found"}
        )
    return record


@app.get("/commands")
def list_commands(limit: int = 50):
    """List the most recent commands (default: last 50)."""
    with _command_lock:
        items = list(_command_log.values())
    items.sort(key=lambda r: r["issued_at"], reverse=True)
    return {
        "commands":     items[:limit],
        "total_logged": len(items),
    }


@app.post("/fault/inject")
def inject_fault():
    global _FAULT_ACTIVE
    _FAULT_ACTIVE = True
    update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", "Service fault injected via API")
    logger.warning(f"[{SERVICE_NAME}] Fault injected")
    return {"message": f"Fault injected into {SERVICE_NAME}", "status": "UNHEALTHY"}


@app.post("/fault/clear")
def clear_fault():
    global _FAULT_ACTIVE
    _FAULT_ACTIVE = False
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "")
    logger.info(f"[{SERVICE_NAME}] Fault cleared")
    return {"message": f"Fault cleared on {SERVICE_NAME}", "status": "HEALTHY"}


@app.get("/fault/status")
def fault_status():
    return {"service": SERVICE_NAME, "fault_active": _FAULT_ACTIVE}


@app.get("/")
def root():
    return {
        "service":     SERVICE_NAME,
        "version":     "1.0.0",
        "description": "Remote vehicle lock/unlock command service (MQTT write path)",
    }
