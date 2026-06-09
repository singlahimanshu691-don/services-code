#!/usr/bin/env python3
"""
vsr/main.py
===========
AutoOps AI – Vehicle Status Report (VSR)

Aggregates raw telemetry into customer-facing business data:
fuel level, mileage, lock status, maintenance summary, etc.
Calls vehicle-telemetry-service for telemetry and presence-service
for online/offline labeling.

This is a parallel read path off vehicle-telemetry — it is NOT in the
diagnostics chain. A vehicle-telemetry outage cascades to both the
diagnostics chain AND to VSR. A presence-service outage cascades only
to telemetry and VSR (not to the diagnostics chain).

Exposed HTTP endpoints:
  GET /health                  → service health + upstream check
  GET /status/{vehicle_id}     → user-friendly vehicle status report
  GET /status                  → service description
"""

import os
import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import httpx
from falkordb import FalkorDB
from graph_utils import update_and_cascade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME    = os.getenv("SERVICE_NAME",    "vsr-service")
UPSTREAM_URL    = os.getenv("UPSTREAM_URL",    "http://localhost:8001")  # vehicle-telemetry
PRESENCE_URL    = os.getenv("PRESENCE_URL",    "http://localhost:8007")
FALKORDB_HOST   = os.getenv("FALKORDB_HOST",   "localhost")
FALKORDB_PORT   = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER   = os.getenv("FALKORDB_USER",   "sampleuser")
FALKORDB_PASS   = os.getenv("FALKORDB_PASS",   "samplePass123")
FALKORDB_GRAPH  = os.getenv("FALKORDB_GRAPH",  "autoops")

_db = None
_graph = None
_FAULT_ACTIVE = False


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_graph()
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "Service started")
    logger.info(f"[{SERVICE_NAME}] Started on port 8008")
    yield
    update_and_cascade(get_graph(), SERVICE_NAME, "UNKNOWN", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="Vehicle Status Report Service", lifespan=lifespan)


@app.get("/health")
def health():
    global _FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("self: service fault injected")

    # Check upstream vehicle-telemetry
    try:
        r = httpx.get(f"{UPSTREAM_URL}/health", timeout=3.0)
        upstream_data = r.json()
        if upstream_data.get("status") != "HEALTHY":
            issues.append(
                f"upstream:vehicle-telemetry-service is {upstream_data.get('status', 'UNKNOWN')}"
            )
    except httpx.TimeoutException:
        issues.append("upstream:vehicle-telemetry-service timed out")
    except Exception as e:
        issues.append(f"upstream:vehicle-telemetry-service unreachable ({type(e).__name__})")

    # Check upstream presence-service
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
            "service":   SERVICE_NAME,
            "status":    status,
            "issues":    issues,
            "timestamp": time.time(),
        }
    )


@app.get("/status/{vehicle_id}")
def get_vehicle_status_report(vehicle_id: str):
    """
    Aggregate telemetry + presence into a user-friendly vehicle status.
    Used by customer-facing apps and the dealer portal.
    """
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    # Pull telemetry
    try:
        r = httpx.get(f"{UPSTREAM_URL}/telemetry/{vehicle_id}", timeout=5.0)
        if r.status_code != 200:
            return JSONResponse(
                status_code=502,
                content={"error": f"vehicle-telemetry-service returned {r.status_code}"}
            )
        telemetry = r.json()
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"error": f"Cannot reach vehicle-telemetry-service: {e}"}
        )

    # Pull presence (best-effort label — failure surfaces in /health, not here)
    online = None
    try:
        r = httpx.get(f"{PRESENCE_URL}/presence/{vehicle_id}", timeout=3.0)
        if r.status_code == 200:
            online = r.json().get("online")
    except Exception:
        online = None  # downgrade gracefully — presence shown as null

    # Derive user-friendly fields from raw telemetry
    fuel_pct        = telemetry.get("fuel_pct", 0)
    odometer_km     = telemetry.get("odometer_km", 0)
    battery_voltage = telemetry.get("battery_voltage", 0.0)
    engine_temp_c   = telemetry.get("engine_temp_c", 0)

    fuel_status    = "LOW" if fuel_pct < 15 else ("MEDIUM" if fuel_pct < 50 else "OK")
    battery_status = "LOW" if battery_voltage < 12.0 else "OK"
    engine_status  = "HOT" if engine_temp_c > 100 else "NORMAL"

    return {
        "vehicle_id":      vehicle_id,
        "online":          online,
        "connectivity":    "ONLINE" if online else ("OFFLINE" if online is False else "UNKNOWN"),
        "fuel": {
            "level_pct": fuel_pct,
            "status":    fuel_status,
        },
        "mileage": {
            "odometer_km": odometer_km,
        },
        "battery": {
            "voltage": battery_voltage,
            "status":  battery_status,
        },
        "engine": {
            "temp_c": engine_temp_c,
            "status": engine_status,
        },
        # lock_status defaults to UNKNOWN — it is owned by rlu-service and
        # would normally be fetched from a dedicated state store.
        "lock_status":     "UNKNOWN",
        "speed_kmh":       telemetry.get("speed_kmh", 0),
        "last_telemetry":  telemetry.get("timestamp"),
        "timestamp":       time.time(),
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
    return {"service": SERVICE_NAME, "version": "1.0.0", "status": "running"}
