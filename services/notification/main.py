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

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "notification-service")
UPSTREAM_URL   = os.getenv("UPSTREAM_URL",   "http://localhost:8003")
MNP_URL        = os.getenv("MNP_URL",        "http://localhost:8010")
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

# Topic on MNP for notification fan-out. Subscribers (mobile app sim,
# dealer push sim, etc.) consume from this topic.
MNP_TOPIC_NOTIFICATIONS = "autoops/notifications/vehicle"

_db = None
_graph = None
_FAULT_ACTIVE = False

# In-memory notification log (simulates sent notifications)
_notification_log = []


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
    logger.info(f"[{SERVICE_NAME}] Started on port 8004")
    yield
    update_and_cascade(get_graph(), SERVICE_NAME, "UNKNOWN", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="Notification Service", lifespan=lifespan)


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
            issues.append(f"upstream:maintenance-service is {upstream_data.get('status', 'UNKNOWN')}")
    except httpx.TimeoutException:
        issues.append("upstream:maintenance-service timed out")
    except Exception as e:
        issues.append(f"upstream:maintenance-service unreachable ({type(e).__name__})")

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


@app.post("/notify/{vehicle_id}")
def send_notification(vehicle_id: str):
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    try:
        r = httpx.get(f"{UPSTREAM_URL}/maintenance/{vehicle_id}", timeout=5.0)
        if r.status_code != 200:
            return JSONResponse(
                status_code=502,
                content={"error": f"Maintenance service returned {r.status_code}"}
            )
        maintenance_data = r.json()
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"error": f"Cannot reach maintenance-service: {e}"}
        )

    tasks = maintenance_data.get("maintenance_tasks", [])
    immediate_tasks = [t for t in tasks if t.get("priority") == "IMMEDIATE"]

    notification = {
        "notification_id": f"NOTIF-{int(time.time())}",
        "vehicle_id":      vehicle_id,
        "channel":         "SMS+EMAIL",
        "recipient":       f"owner-{vehicle_id}@automotive.com",
        "subject":         f"Vehicle {vehicle_id} — Maintenance Alert",
        "urgent":          len(immediate_tasks) > 0,
        "message": (
            f"URGENT: {len(immediate_tasks)} critical issue(s) require immediate attention."
            if immediate_tasks
            else f"Scheduled maintenance due: {len(tasks)} task(s) pending."
        ),
        "tasks_summary": [
            {"code": t["dtc_code"], "task": t["maintenance_task"], "priority": t["priority"]}
            for t in tasks
        ],
        "sent_at": time.time(),
        "status":  "SENT"
    }

    _notification_log.append(notification)
    logger.info(f"[{SERVICE_NAME}] Notification sent for vehicle {vehicle_id}")

    # ── Best-effort fan-out to MNP (OPTIONAL — must not fail this request) ──
    # The DEPENDS_ON edge to mnp-service is marked `required: false` in the
    # dependency graph. A failure here is logged on the response and in the
    # service log, but never raises and never affects notification's status.
    mnp_publish_status = "skipped"
    try:
        r = httpx.post(
            f"{MNP_URL}/publish/{MNP_TOPIC_NOTIFICATIONS}",
            json=notification,
            timeout=2.0,
        )
        if r.status_code == 200:
            data = r.json()
            mnp_publish_status = (
                f"delivered to {data.get('delivered_subscribers', 0)} subscriber(s)"
            )
        else:
            mnp_publish_status = f"failed (mnp returned {r.status_code})"
            logger.warning(f"[{SERVICE_NAME}] MNP publish returned {r.status_code}")
    except httpx.TimeoutException:
        mnp_publish_status = "failed (mnp timed out)"
        logger.warning(f"[{SERVICE_NAME}] MNP publish timed out — fan-out skipped")
    except Exception as e:
        mnp_publish_status = f"failed ({type(e).__name__})"
        logger.warning(f"[{SERVICE_NAME}] MNP publish error — fan-out skipped: {e}")

    notification["mnp_publish_status"] = mnp_publish_status
    return notification


@app.get("/notifications")
def get_notifications():
    return {"notifications": _notification_log, "total": len(_notification_log)}


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