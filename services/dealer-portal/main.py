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

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "dealer-portal-service")
UPSTREAM_URL   = os.getenv("UPSTREAM_URL",   "http://localhost:8004")
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

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
    logger.info(f"[{SERVICE_NAME}] Started on port 8005")
    yield
    update_and_cascade(get_graph(), SERVICE_NAME, "UNKNOWN", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="Dealer Portal Service", lifespan=lifespan)


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
            issues.append(f"upstream:notification-service is {upstream_data.get('status', 'UNKNOWN')}")
    except httpx.TimeoutException:
        issues.append("upstream:notification-service timed out")
    except Exception as e:
        issues.append(f"upstream:notification-service unreachable ({type(e).__name__})")

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


@app.get("/dealer/dashboard/{dealer_id}")
def get_dealer_dashboard(dealer_id: str):
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    try:
        r = httpx.get(f"{UPSTREAM_URL}/notifications", timeout=5.0)
        if r.status_code != 200:
            return JSONResponse(
                status_code=502,
                content={"error": f"Notification service returned {r.status_code}"}
            )
        notif_data = r.json()
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"error": f"Cannot reach notification-service: {e}"}
        )

    notifications = notif_data.get("notifications", [])
    urgent = [n for n in notifications if n.get("urgent")]

    return {
        "dealer_id": dealer_id,
        "dashboard": {
            "total_alerts":                  len(notifications),
            "urgent_alerts":                 len(urgent),
            "vehicles_requiring_attention":  list({n["vehicle_id"] for n in urgent}),
            "recent_notifications":          notifications[-5:] if notifications else [],
        },
        "dealer_info": {
            "name":           f"AutoCare Dealer {dealer_id}",
            "region":         "Pune, Maharashtra",
            "service_center": "authorized"
        },
        "system_health": "DEGRADED" if urgent else "NOMINAL",
        "timestamp": time.time()
    }


@app.get("/dealer/vehicles")
def list_vehicles():
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    return {
        "vehicles": [
            {"id": "VH-1042", "model": "Mahindra XUV700", "year": 2022, "status": "ALERT"},
            {"id": "VH-2087", "model": "Tata Nexon EV",   "year": 2023, "status": "OK"},
            {"id": "VH-3319", "model": "Hyundai Creta",   "year": 2021, "status": "SERVICE_DUE"},
            {"id": "VH-4456", "model": "Kia Seltos",      "year": 2023, "status": "OK"},
        ],
        "total":     4,
        "timestamp": time.time()
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