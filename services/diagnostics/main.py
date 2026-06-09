import os
import sqlite3
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

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "diagnostics-service")
UPSTREAM_URL   = os.getenv("UPSTREAM_URL",   "http://localhost:8001")
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")
DB_PATH        = os.getenv("DB_PATH",        "/app/data/dtc_database.db")

_db = None
_graph = None
_FAULT_ACTIVE        = False
_DB_FAULT_ACTIVE     = False
_SILENT_FAULT_ACTIVE = False  # Use Case 4: logs errors, graph stays HEALTHY


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


def update_datastore_state(state: str, message: str = ""):
    g = get_graph()
    if g is None:
        return
    try:
        g.query(
            """
            MATCH (ds:Datastore {name: 'sqlite-dtc-db'})
            SET ds.state = $state, ds.lastUpdated = timestamp(), ds.message = $message
            """,
            {"state": state, "message": message}
        )
    except Exception as e:
        logger.warning(f"[{SERVICE_NAME}] FalkorDB datastore state update failed: {e}")


def get_db_connection():
    """Returns a SQLite connection. Raises exception if DB fault is active."""
    if _DB_FAULT_ACTIVE:
        raise Exception("SQLite DTC database is unavailable (fault injected)")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the SQLite database with DTC codes."""
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dtc_codes (
                code TEXT PRIMARY KEY,
                description TEXT NOT NULL,
                severity TEXT NOT NULL,
                system TEXT NOT NULL,
                recommended_action TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s','now'))
            )
        """)
        dtc_data = [
            ("P0300", "Random/Multiple Cylinder Misfire Detected",       "HIGH",     "Engine",      "Inspect spark plugs, ignition coils, and fuel injectors"),
            ("P0171", "System Too Lean (Bank 1)",                        "MEDIUM",   "Fuel",        "Check fuel injectors, MAF sensor, and vacuum leaks"),
            ("P0420", "Catalyst System Efficiency Below Threshold",      "LOW",      "Exhaust",     "Inspect catalytic converter and oxygen sensors"),
            ("P0113", "Intake Air Temperature Sensor High Input",        "LOW",      "Air Intake",  "Replace IAT sensor or check wiring harness"),
            ("B0001", "Airbag Deployment Loop Open",                     "CRITICAL", "Safety",      "Inspect airbag circuit and clockspring immediately"),
            ("C0035", "Left Front Wheel Speed Sensor Circuit",           "HIGH",     "ABS",         "Replace wheel speed sensor or repair wiring"),
            ("P0442", "Evaporative Emission Control System Leak Small",  "LOW",      "EVAP",        "Check fuel cap, purge valve, and EVAP lines"),
            ("P0562", "System Voltage Low",                              "HIGH",     "Electrical",  "Test alternator, battery, and charging circuit"),
            ("U0100", "Lost Communication With ECM/PCM",                 "CRITICAL", "Network",     "Check CAN bus wiring and ECM power supply"),
            ("P0128", "Coolant Temperature Below Thermostat Regulating", "MEDIUM",   "Cooling",     "Replace engine thermostat"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO dtc_codes (code, description, severity, system, recommended_action) VALUES (?,?,?,?,?)",
            dtc_data
        )
        conn.commit()
        conn.close()
        logger.info(f"[{SERVICE_NAME}] SQLite DTC database initialized at {DB_PATH}")
        update_datastore_state("HEALTHY", "Database initialized")
    except Exception as e:
        logger.error(f"[{SERVICE_NAME}] DB init failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_graph()
    init_db()
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "Service started")
    logger.info(f"[{SERVICE_NAME}] Started on port 8002")
    yield
    update_and_cascade(get_graph(), SERVICE_NAME, "UNKNOWN", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="Diagnostics Service", lifespan=lifespan)


@app.get("/health")
def health():
    global _FAULT_ACTIVE, _DB_FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("self: service fault injected")

    try:
        r = httpx.get(f"{UPSTREAM_URL}/health", timeout=3.0)
        upstream_data = r.json()
        if upstream_data.get("status") != "HEALTHY":
            issues.append(f"upstream:vehicle-telemetry-service is {upstream_data.get('status', 'UNKNOWN')}")
    except httpx.TimeoutException:
        issues.append("upstream:vehicle-telemetry-service timed out")
    except Exception as e:
        issues.append(f"upstream:vehicle-telemetry-service unreachable ({type(e).__name__})")

    if _DB_FAULT_ACTIVE:
        issues.append("dependency:sqlite-dtc-db fault injected — database unavailable")
        update_datastore_state("UNHEALTHY", "Fault injected via API")
    else:
        try:
            conn = get_db_connection()
            conn.execute("SELECT COUNT(*) FROM dtc_codes").fetchone()
            conn.close()
            update_datastore_state("HEALTHY", "")
        except Exception as e:
            issues.append(f"dependency:sqlite-dtc-db error ({e})")
            update_datastore_state("UNHEALTHY", str(e))

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


@app.get("/diagnostics/{vehicle_id}")
def get_diagnostics(vehicle_id: str):
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    try:
        r = httpx.get(f"{UPSTREAM_URL}/telemetry/{vehicle_id}", timeout=3.0)
        if r.status_code != 200:
            return JSONResponse(status_code=502, content={"error": "Upstream telemetry service returned error"})
        telemetry = r.json()
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": f"Cannot reach vehicle-telemetry-service: {e}"})

    try:
        conn = get_db_connection()
        dtcs = conn.execute(
            "SELECT * FROM dtc_codes WHERE severity IN ('HIGH', 'CRITICAL') ORDER BY severity DESC"
        ).fetchall()
        all_dtcs = conn.execute("SELECT COUNT(*) as total FROM dtc_codes").fetchone()
        conn.close()
        return {
            "vehicle_id":            vehicle_id,
            "telemetry":             telemetry,
            "active_dtcs":           [dict(d) for d in dtcs],
            "total_dtc_codes_in_db": all_dtcs["total"],
            "timestamp":             time.time()
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": f"DTC database error: {e}"})


@app.get("/dtc-codes")
def get_all_dtc_codes():
    try:
        conn = get_db_connection()
        codes = conn.execute("SELECT * FROM dtc_codes ORDER BY severity DESC").fetchall()
        conn.close()
        return {"dtc_codes": [dict(c) for c in codes], "count": len(codes)}
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": f"DTC database error: {e}"})


@app.post("/fault/inject")
def inject_fault():
    global _FAULT_ACTIVE
    _FAULT_ACTIVE = True
    update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", "Service fault injected via API")
    logger.warning(f"[{SERVICE_NAME}] Service fault injected")
    return {"message": f"Service fault injected into {SERVICE_NAME}", "status": "UNHEALTHY"}


@app.post("/fault/inject-db")
def inject_db_fault():
    global _DB_FAULT_ACTIVE
    _DB_FAULT_ACTIVE = True
    update_and_cascade(get_graph(), SERVICE_NAME, "UNHEALTHY", "SQLite DTC database fault injected")
    update_datastore_state("UNHEALTHY", "Fault injected via API")
    logger.warning(f"[{SERVICE_NAME}] DB fault injected — DTC database unavailable")
    return {
        "message":          "SQLite DTC database fault injected — diagnostics-service will fail DTC lookups",
        "root_cause":       "sqlite-dtc-db",
        "affected_service": SERVICE_NAME
    }


@app.post("/fault/clear")
def clear_fault():
    global _FAULT_ACTIVE, _DB_FAULT_ACTIVE, _SILENT_FAULT_ACTIVE
    _FAULT_ACTIVE        = False
    _DB_FAULT_ACTIVE     = False
    _SILENT_FAULT_ACTIVE = False
    update_and_cascade(get_graph(), SERVICE_NAME, "HEALTHY", "")
    update_datastore_state("HEALTHY", "Fault cleared")
    logger.info(f"[{SERVICE_NAME}] All faults cleared")
    return {"message": f"All faults cleared on {SERVICE_NAME}", "status": "HEALTHY"}


@app.get("/fault/status")
def fault_status():
    return {
        "service":               SERVICE_NAME,
        "service_fault_active":  _FAULT_ACTIVE,
        "db_fault_active":       _DB_FAULT_ACTIVE,
        "silent_fault_active":   _SILENT_FAULT_ACTIVE,
    }


@app.post("/fault/inject-silent")
def inject_silent_fault():
    """
    Use Case 4 — Silent fault injection for log analysis demo.

    Starts a background thread that repeatedly attempts DTC database lookups
    and logs real ERROR entries when they fail (by temporarily enabling the DB
    fault internally). The /health endpoint is NOT affected — it continues
    returning HEALTHY and FalkorDB graph state stays HEALTHY throughout.

    This simulates an unknown intermittent failure that only surfaces in logs,
    not in health checks or the dependency graph. The AI agent must use
    get_service_logs() or get_all_service_logs() to discover and explain it.

    Reset with: POST /fault/clear-silent
    """
    global _SILENT_FAULT_ACTIVE
    _SILENT_FAULT_ACTIVE = True

    def _generate_error_logs():
        """Background thread: simulates intermittent DTC lookup failures."""
        import random
        error_count = 0
        vehicle_ids = ["VH-1001", "VH-1002", "VH-1003", "VH-1004", "VH-1005"]
        dtc_codes   = ["P0300", "P0171", "B0001", "U0100", "C0035"]

        while _SILENT_FAULT_ACTIVE and error_count < 20:
            vid = random.choice(vehicle_ids)
            dtc = random.choice(dtc_codes)

            try:
                conn = sqlite3.connect("/app/data/nonexistent_dtc.db")
                conn.execute(
                    "SELECT description, severity FROM dtc_codes WHERE code = ?", (dtc,)
                ).fetchone()
                conn.close()
            except Exception as e:
                logger.error(
                    f"[{SERVICE_NAME}] Failed to retrieve DTC record for {dtc} "
                    f"(vehicle {vid}) — database read error: {e}"
                )

            error_count += 1
            time.sleep(3)

        if error_count >= 20:
            logger.warning(
                f"[{SERVICE_NAME}] Repeated DTC lookup failures observed "
                f"({error_count} occurrences) — database stability degraded"
            )

    import threading
    t = threading.Thread(target=_generate_error_logs, daemon=True, name="silent-fault")
    t.start()

    logger.warning(
        f"[{SERVICE_NAME}] Intermittent DTC database errors detected — "
        f"initiating diagnostic monitoring"
    )
    return {
        "message": f"Diagnostic monitoring started on {SERVICE_NAME}",
        "service": SERVICE_NAME,
        "status":  "monitoring",
    }


@app.post("/fault/clear-silent")
def clear_silent_fault():
    """Stop the diagnostic monitoring thread."""
    global _SILENT_FAULT_ACTIVE
    _SILENT_FAULT_ACTIVE = False
    logger.info(f"[{SERVICE_NAME}] DTC diagnostic monitoring stopped")
    return {"message": f"Diagnostic monitoring stopped on {SERVICE_NAME}", "service": SERVICE_NAME}


@app.get("/")
def root():
    return {"service": SERVICE_NAME, "version": "1.0.0", "status": "running"}