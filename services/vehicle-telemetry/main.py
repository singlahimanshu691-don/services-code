import os
import time
import random
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from falkordb import FalkorDB
from graph_utils import update_and_cascade
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME      = os.getenv("SERVICE_NAME",      "vehicle-telemetry-service")
FALKORDB_HOST     = os.getenv("FALKORDB_HOST",     "localhost")
FALKORDB_PORT     = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER     = os.getenv("FALKORDB_USER",     "sampleuser")
FALKORDB_PASS     = os.getenv("FALKORDB_PASS",     "samplePass123")
FALKORDB_GRAPH    = os.getenv("FALKORDB_GRAPH",    "autoops")
PRESENCE_URL      = os.getenv("PRESENCE_URL",      "http://localhost:8007")

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
    # startup
    get_graph()
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "Service started")
    logger.info(f"[{SERVICE_NAME}] Started on port 8001")
    yield
    # shutdown
    update_and_cascade(get_graph(), SERVICE_NAME, "UNKNOWN", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="Vehicle Telemetry Service", lifespan=lifespan)


@app.get("/health")
def health():
    global _FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("service fault injected")

    # Check upstream presence-service dependency
    try:
        r = httpx.get(f"{PRESENCE_URL}/health", timeout=3.0)
        if r.status_code != 200:
            issues.append(f"upstream:presence-service is UNHEALTHY")
    except httpx.ConnectError:
        issues.append("upstream:presence-service unreachable")
    except httpx.TimeoutException:
        issues.append("upstream:presence-service timed out")

    status = "UNHEALTHY" if issues else "HEALTHY"
    update_and_cascade(get_graph(), SERVICE_NAME, status, "; ".join(issues) if issues else "")

    return JSONResponse(
        status_code=503 if status == "UNHEALTHY" else 200,
        content={
            "service":   SERVICE_NAME,
            "status":    status,
            "issues":    issues,
            "timestamp": time.time(),
        }
    )


@app.get("/telemetry")
def get_telemetry():
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(
            status_code=503,
            content={"error": "Service unavailable — fault injected"}
        )
    return {
        "vehicle_id":        f"VH-{random.randint(1000, 9999)}",
        "speed_kmh":         random.randint(0, 140),
        "rpm":               random.randint(700, 6000),
        "fuel_pct":          random.randint(5, 100),
        "engine_temp_c":     random.randint(70, 105),
        "oil_pressure_psi":  random.randint(25, 80),
        "battery_voltage":   round(random.uniform(11.5, 14.8), 2),
        "odometer_km":       random.randint(5000, 200000),
        "timestamp":         time.time()
    }


@app.get("/telemetry/{vehicle_id}")
def get_telemetry_for_vehicle(vehicle_id: str):
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(
            status_code=503,
            content={"error": "Service unavailable — fault injected"}
        )
    return {
        "vehicle_id":        vehicle_id,
        "speed_kmh":         random.randint(0, 140),
        "rpm":               random.randint(700, 6000),
        "fuel_pct":          random.randint(5, 100),
        "engine_temp_c":     random.randint(70, 105),
        "oil_pressure_psi":  random.randint(25, 80),
        "battery_voltage":   round(random.uniform(11.5, 14.8), 2),
        "odometer_km":       random.randint(5000, 200000),
        "timestamp":         time.time()
    }


@app.post("/fault/inject")
def inject_fault():
    global _FAULT_ACTIVE
    _FAULT_ACTIVE = True
    update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", "Fault injected via API")
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