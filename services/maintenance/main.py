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

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "maintenance-service")
UPSTREAM_URL   = os.getenv("UPSTREAM_URL",   "http://localhost:8002")
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

_db = None
_graph = None
_FAULT_ACTIVE = False

MAINTENANCE_SCHEDULE = {
    "P0300": {"interval_km": 10000, "task": "Replace spark plugs and inspect ignition system"},
    "P0171": {"interval_km": 15000, "task": "Clean/replace fuel injectors, inspect MAF sensor"},
    "P0420": {"interval_km": 80000, "task": "Replace catalytic converter"},
    "P0113": {"interval_km": 30000, "task": "Replace IAT sensor"},
    "C0035": {"interval_km": 40000, "task": "Replace wheel speed sensor"},
    "P0562": {"interval_km": 20000, "task": "Inspect alternator and battery"},
    "DEFAULT": {"interval_km": 5000,  "task": "General inspection required"}
}


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
    logger.info(f"[{SERVICE_NAME}] Started on port 8003")
    yield
    update_and_cascade(get_graph(), SERVICE_NAME, "UNKNOWN", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="Maintenance Service", lifespan=lifespan)


@app.get("/health")
def health():
    global _FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("self: service fault injected")

    try:
        r = httpx.get(f"{UPSTREAM_URL}/health", timeout=3.0)
        upstream_data = r.json()
        if upstream_data.get("status") != "HEALTHY":
            issues.append(f"upstream:diagnostics-service is {upstream_data.get('status', 'UNKNOWN')}")
    except httpx.TimeoutException:
        issues.append("upstream:diagnostics-service timed out")
    except Exception as e:
        issues.append(f"upstream:diagnostics-service unreachable ({type(e).__name__})")

    status = "UNHEALTHY" if issues else "HEALTHY"
    message = "; ".join(issues) if issues else ""
    update_and_cascade(get_graph(), SERVICE_NAME, status, message)

    return JSONResponse(
        status_code=503 if status == "UNHEALTHY" else 200,
        content={
            "service":   SERVICE_NAME,
            "status":    status,
            "issues":    issues,
            "timestamp": time.time()
        }
    )


@app.get("/maintenance/{vehicle_id}")
def get_maintenance_schedule(vehicle_id: str):
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    try:
        r = httpx.get(f"{UPSTREAM_URL}/diagnostics/{vehicle_id}", timeout=5.0)
        if r.status_code != 200:
            return JSONResponse(
                status_code=502,
                content={"error": f"Diagnostics service returned {r.status_code}"}
            )
        diag_data = r.json()
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"error": f"Cannot reach diagnostics-service: {e}"}
        )

    active_dtcs = diag_data.get("active_dtcs", [])
    tasks = []
    for dtc in active_dtcs:
        code = dtc.get("code", "")
        schedule = MAINTENANCE_SCHEDULE.get(code, MAINTENANCE_SCHEDULE["DEFAULT"])
        tasks.append({
            "dtc_code":         code,
            "description":      dtc.get("description", ""),
            "severity":         dtc.get("severity", ""),
            "maintenance_task": schedule["task"],
            "next_service_km":  schedule["interval_km"],
            "priority": "IMMEDIATE" if dtc.get("severity") in ("CRITICAL", "HIGH") else "SCHEDULED"
        })

    return {
        "vehicle_id":        vehicle_id,
        "maintenance_tasks": tasks,
        "total_tasks":       len(tasks),
        "next_service_due":  "Immediate" if any(t["priority"] == "IMMEDIATE" for t in tasks) else "Scheduled",
        "timestamp":         time.time()
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