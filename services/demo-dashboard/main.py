"""
demo-dashboard/main.py
======================
FastAPI backend for the AutoOps AI PoC demo dashboard.

Runs inside Docker alongside all other services.
Exposes:
  GET  /api/health          — all service health states
  GET  /api/graph           — FalkorDB node states + RCA candidates
  GET  /api/containers      — Docker container states
  GET  /api/vehicles        — vehicle fleet state from presence-service
  GET  /api/logs/{service}  — recent docker logs for a service
  POST /api/fault/{action}  — inject / clear / status per scenario
  POST /api/seed/{mode}     — seed_falkordb.py wrapper
  POST /api/simulate/{cmd}  — simulate_vehicles.py wrapper
  GET  /                    — serves the dashboard HTML

All inter-service calls use internal Docker network names.
"""
from scripts.trigger_kai import trigger_kai

import os
import subprocess
import time
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from falkordb import FalkorDB
from fastapi.staticfiles import StaticFiles


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Service URLs (internal Docker network) ────────────────────────────────────
SERVICES = {
    "vehicle-telemetry-service": "http://vehicle-telemetry:8001",
    "diagnostics-service":       "http://diagnostics:8002",
    "maintenance-service":       "http://maintenance:8003",
    "notification-service":      "http://notification:8004",
    "dealer-portal-service":     "http://dealer-portal:8005",
    "presence-service":          "http://presence:8007",
    "vsr-service":               "http://vsr:8008",
    "rlu-service":               "http://rlu:8009",
    "mnp-service":               "http://mnp:8010",
}

CONTAINER_NAMES = {
    "vehicle-telemetry-service": "vehicle-telemetry",
    "diagnostics-service":       "diagnostics",
    "maintenance-service":       "maintenance",
    "notification-service":      "notification",
    "dealer-portal-service":     "dealer-portal",
    "presence-service":          "presence",
    "vsr-service":               "vsr",
    "rlu-service":               "rlu",
    "mnp-service":               "mnp",
    "mosquitto-broker":          "mosquitto",
    "falkordb":                  "falkordb",
}

FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

# Path to scripts inside the container (mounted from project root)
SCRIPTS_DIR = Path("/app/scripts")

mailer_process = None


# ── FalkorDB helper ───────────────────────────────────────────────────────────
def falkordb_query(cypher: str, params: dict = None):
    try:
        db = FalkorDB(
            host=FALKORDB_HOST,
            port=FALKORDB_PORT,
            username=FALKORDB_USER,
            password=FALKORDB_PASS,
        )
        g = db.select_graph(FALKORDB_GRAPH)
        result = g.query(cypher, params or {})

        # Header format is [type_int, 'column_name'] → use index 1
        column_names = []
        for h in result.header:
            if isinstance(h, (list, tuple)):
                column_names.append(str(h[1]))  # ← was h[0], now h[1]
            else:
                column_names.append(str(h))

        rows = []
        for row in result.result_set:
            # Rows are nested: [['value1', 'value2']] → flatten first
            flat_row = row[0] if (isinstance(row, list) and len(row) == 1 and isinstance(row[0], list)) else row
            record = {key: value for key, value in zip(column_names, flat_row)}
            rows.append(record)

        db.connection.close()
        return rows

    except Exception as e:
        logger.warning(f"FalkorDB query failed: {e}")
        return []


# ── App ───────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Demo dashboard started on port 8090")
    yield

app = FastAPI(title="AutoOps Demo Dashboard", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/images", StaticFiles(directory="images"), name="images")


# ══════════════════════════════════════════════════════════════════════════════
# API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/health")
async def get_all_health():
    """Poll /health on all services simultaneously."""
    results = {}
    async with httpx.AsyncClient(timeout=4.0) as client:
        for name, url in SERVICES.items():
            try:
                r = await client.get(f"{url}/health")
                data = r.json()
                data["http_status"] = r.status_code
                results[name] = data
            except httpx.ConnectError:
                results[name] = {"service": name, "status": "UNREACHABLE",
                                 "issues": ["connection refused"], "http_status": 0}
            except httpx.TimeoutException:
                results[name] = {"service": name, "status": "UNREACHABLE",
                                 "issues": ["health check timed out"], "http_status": 0}
            except Exception as e:
                results[name] = {"service": name, "status": "ERROR",
                                 "issues": [str(e)], "http_status": 0}
    return results


@app.get("/api/fault-status")
async def get_fault_status():
    """Get fault flag state for all services."""
    results = {}
    async with httpx.AsyncClient(timeout=3.0) as client:
        for name, url in SERVICES.items():
            try:
                r = await client.get(f"{url}/fault/status")
                results[name] = r.json()
            except Exception:
                results[name] = {"error": "unreachable"}
    return results


@app.get("/api/graph")
async def get_graph_state():
    """Return FalkorDB node states, vehicle counts, Jira ticket count, RCA candidates."""
    services = falkordb_query(
        "MATCH (s:Service) RETURN s.name AS name, s.status AS status, "
        "s.criticality AS criticality, s.message AS message ORDER BY s.criticality"
    )
    datastores = falkordb_query(
        "MATCH (d:Datastore) RETURN d.name AS name, d.status AS status, d.message AS message"
    )
    broker = falkordb_query(
        "MATCH (b:MQTTBroker) RETURN b.name AS name, b.status AS status"
    )
    vehicles = falkordb_query(
        "MATCH (v:Vehicle) RETURN v.state AS state, count(v) AS count"
    )
    tickets = falkordb_query("MATCH (t:JiraTicket) RETURN count(t) AS count")
    rca = falkordb_query(
        """
        MATCH (n) 
        WHERE (n:Service OR n:Datastore) 
        AND n.status = 'UNHEALTHY'
        AND NOT (
        (n)-[:DEPENDS_ON]->(:Service {status: 'UNHEALTHY'}) OR 
        (n)-[:DEPENDS_ON]->(:Datastore {status: 'UNHEALTHY'})
  )
RETURN n.name AS name, labels(n)[0] AS type, n.message AS message

        """
    )
    return {
        "services":       services,
        "datastores":     datastores,
        "broker":         broker,
        "vehicles":       vehicles,
        "jira_count":     tickets[0]["count"] if tickets else 0,
        "rca_candidates": rca,
    }


@app.get("/api/containers")
def get_container_states():
    """Get Docker container running/stopped states."""
    states = {}
    for name, container in CONTAINER_NAMES.items():
        try:
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Status}}", container],
                capture_output=True, text=True, timeout=5
            )
            states[name] = result.stdout.strip() if result.returncode == 0 else "not found"
        except Exception:
            states[name] = "error"
    return states


@app.get("/api/logs/{service_name}")
def get_service_logs(service_name: str, lines: int = 60):
    """Return recent docker logs for a service, noise-filtered."""
    container = CONTAINER_NAMES.get(service_name)
    if not container:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service_name}")

    noise = ("GET /health", "GET /fault/status", "GET /metrics",
             "uvicorn.access", "200 OK", "Health check",
             "Application startup", "Started server")
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", str(lines), "--timestamps", container],
            capture_output=True, text=True, timeout=10
        )
        raw = (result.stdout + result.stderr).strip()
        all_lines = [l for l in raw.splitlines() if l.strip()]
        signal_lines = [l for l in all_lines if not any(n in l for n in noise)]
        errors   = [l for l in signal_lines if any(k in l for k in
                    ("ERROR", "CRITICAL", "EXCEPTION", "Traceback", "fatal"))]
        warnings = [l for l in signal_lines if any(k in l for k in ("WARNING", "WARN"))
                    and not any(k in l for k in ("ERROR", "CRITICAL"))]
        return {
            "service":       service_name,
            "container":     container,
            "total_lines":   len(all_lines),
            "signal_lines":  signal_lines[-40:],
            "errors":        errors[-20:],
            "warnings":      warnings[-10:],
            "error_count":   len(errors),
            "warning_count": len(warnings),
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="docker logs timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/vehicles")
async def get_vehicles():
    """Get vehicle fleet state from presence-service (owns the :Vehicle graph)."""
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get("http://presence:8007/graph/vehicles")
            return r.json()
    except Exception:
        return {"vehicle_count": 0, "vehicles": [], "error": "presence-service unreachable"}


# ── Fault injection endpoints ─────────────────────────────────────────────────

@app.post("/api/fault/inject/{service}")
async def inject_fault(service: str, ai_mode: bool = False):
    url = SERVICES.get(service)
    if not url:
        raise HTTPException(status_code=404, detail="Unknown service")

    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.post(f"{url}/fault/inject")

    if ai_mode:
        scenario_map = {
            "presence-service":          "1",
            "diagnostics-service":       "2",
            "maintenance-service":       "3",
            "vehicle-telemetry-service": "4",
        }
        scenario_id = scenario_map.get(service)
        if scenario_id:
            trigger_kai(scenario_id)

    return r.json()


@app.post("/api/fault/inject-db")
async def inject_db_fault(ai_mode: bool = False):
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.post("http://diagnostics:8002/fault/inject-db")

    if ai_mode:
        trigger_kai("2")

    return r.json()


@app.post("/api/fault/inject-silent")
async def inject_silent_fault(ai_mode: bool = False):
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.post("http://diagnostics:8002/fault/inject-silent")

    if ai_mode:
        trigger_kai("5")

    return r.json()


@app.post("/api/fault/clear-all")
async def clear_all_faults():
    """
    Reset the demo system to a clean state WITHOUT wiping the graph:
      1. Clear fault flags on all microservices (HTTP calls, unchanged)
      2. Reset Service / Datastore / MQTTBroker nodes to HEALTHY in FalkorDB
      3. Delete ALL Vehicle nodes (both CONNECTED and DISCONNECTED) from the graph
    Everything else — Services, Datastores, Remediations, JiraTickets — is untouched.
    """
    results = {}

    # ── Step 1: Clear fault flags on all microservices ─────────────────────────
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in SERVICES.items():
            try:
                r = await client.post(f"{url}/fault/clear")
                results[name] = r.json()
            except Exception as e:
                results[name] = {"error": str(e)}

    # ── Step 2 & 3: Reset graph health + remove all vehicle nodes ──────────────
    try:
        db = FalkorDB(
            host=FALKORDB_HOST,
            port=FALKORDB_PORT,
            username=FALKORDB_USER,
            password=FALKORDB_PASS,
        )
        g = db.select_graph(FALKORDB_GRAPH)

        # Reset all Service nodes to HEALTHY
        g.query(
            "MATCH (s:Service) "
            "SET s.status = 'HEALTHY', s.message = '', s.lastUpdated = timestamp()"
        )

        # Reset Datastore nodes to HEALTHY
        g.query(
            "MATCH (d:Datastore) "
            "SET d.status = 'HEALTHY', d.message = '', d.lastUpdated = timestamp()"
        )

        # Reset MQTTBroker node to HEALTHY
        g.query(
            "MATCH (b:MQTTBroker) "
            "SET b.status = 'HEALTHY', b.lastUpdated = timestamp()"
        )

        # Count vehicles before deletion (for the response payload)
        v_count_r = g.query("MATCH (v:Vehicle) RETURN count(v) AS c")
        v_count = v_count_r.result_set[0][0] if v_count_r.result_set else 0

        # Remove ALL vehicle nodes — DETACH also removes CONNECTED_TO /
        # DISCONNECTED_FROM edges automatically
        g.query("MATCH (v:Vehicle) DETACH DELETE v")

        db.connection.close()

        results["__graph_reset"] = {
            "success": True,
            "vehicles_removed": v_count,
            "message": (
                f"Removed {v_count} vehicle node(s). "
                "Services, Datastores and MQTTBroker reset to HEALTHY. "
                "All other graph nodes are untouched."
            ),
        }
        logger.info(
            "[Reset] Graph reset complete — %d vehicle(s) removed, all nodes HEALTHY",
            v_count,
        )

    except Exception as e:
        logger.error("[Reset] Graph reset failed: %s", e)
        results["__graph_reset"] = {"success": False, "error": str(e)}

    return results


# ── Seed / simulate wrappers ──────────────────────────────────────────────────

@app.post("/api/seed/{mode}")
def run_seed(mode: str):
    """
    mode: full | no-tickets | tickets-only
    Runs seed_falkordb.py with the appropriate flag.
    """
    flag_map = {
        "full":         [],
        "no-tickets":   ["--no-tickets"],
        "tickets-only": ["--tickets-only"],
    }
    flags = flag_map.get(mode)
    if flags is None:
        raise HTTPException(status_code=400, detail=f"Unknown mode: {mode}")

    cmd = [
        "python3", str(SCRIPTS_DIR / "seed_falkordb.py"),
        "--host",     FALKORDB_HOST,
        "--port",     str(FALKORDB_PORT),
        "--user",     FALKORDB_USER,
        "--password", FALKORDB_PASS,
        "--graph",    FALKORDB_GRAPH,
        "--no-wait",
    ] + flags

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return {
            "success": result.returncode == 0,
            "stdout":  result.stdout[-1000:],
            "stderr":  result.stderr[-500:],
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="seed script timed out")


@app.post("/api/simulate/vehicles/{scenario}")
def simulate_vehicles(scenario: str):
    """Run simulate_vehicles.py in background (non-blocking)."""
    valid = ("wave", "rush_hour", "random", "single", "mass_disconnect")
    if scenario not in valid:
        raise HTTPException(status_code=400, detail=f"Unknown scenario: {scenario}")

    script_path = SCRIPTS_DIR / "simulate_vehicles.py"
    if not script_path.exists():
        raise HTTPException(status_code=500, detail=f"Script not found at {script_path}")

    try:
        cmd = [
            "python3",
            str(script_path),
            "--scenario", scenario,
            "--host", "mosquitto",
            "--port", "1883",
        ]

        subprocess.Popen(
            cmd,
            stdout=open("/tmp/simulate_out.log", "a"),
            stderr=open("/tmp/simulate_err.log", "a"),
        )

        return {
            "message": f"Vehicle simulation '{scenario}' started",
            "script":  str(script_path),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Serve dashboard HTML ──────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>dashboard.html not found</h1>", status_code=500)


@app.post("/api/kai/trigger/{scenario_id}")
def trigger_kai_api(scenario_id: str):
    result = trigger_kai(scenario_id)
    return result


@app.post("/api/run/mailer")
def run_mailer():
    global mailer_process

    if mailer_process is not None and mailer_process.poll() is None:
        return {"message": "Notification poller already running", "pid": mailer_process.pid}

    script = Path("/app/kaii_mailer.py")
    if not script.exists():
        raise HTTPException(status_code=404, detail="kaii_mailer.py not found")

    logger.info("[MAILER] Starting notification poller")
    mailer_process = subprocess.Popen(
        ["python3", str(script)],
        cwd="/app/mcp_server",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    logger.info(f"[MAILER] Process started with PID {mailer_process.pid}")

    time.sleep(2)
    output_lines = []
    try:
        import select
        while True:
            ready, _, _ = select.select([mailer_process.stdout], [], [], 0.5)
            if not ready:
                break
            line = mailer_process.stdout.readline()
            if not line:
                break
            output_lines.append(line.decode().strip())
    except Exception as e:
        logger.warning(f"[MAILER] Could not read initial output: {e}")

    return {
        "message": f"Notification poller started (PID {mailer_process.pid})",
        "pid":     mailer_process.pid,
        "output":  output_lines[-10:] if output_lines else ["Poller running in background..."],
    }