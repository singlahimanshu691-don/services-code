#!/usr/bin/env python3
"""
mnp/main.py
===========
AutoOps AI – Messaging & Notification Platform (MNP)

Lightweight in-memory pub/sub broker for asynchronous event delivery.
Topics are namespaces; messages are JSON; subscribers register by id
and consume via long-poll (or instant if messages are queued).

This is Option A: MNP runs *alongside* the existing HTTP chain
(diagnostics → maintenance → notification → dealer-portal). Notification
publishes events to MNP for additional async consumers (mobile apps,
dealer push, etc.) without disturbing the synchronous read path.

Failure model:
  - MNP UNHEALTHY does NOT cascade to notification-service —
    the publish edge is marked `criticality: optional` in the graph.
  - Queue backlog and consumer lag are exposed via /metrics for the
    agent to reason about.

Exposed HTTP endpoints:
  GET  /health                        → service health
  POST /publish/{topic}               → publish a message (body: any JSON)
  POST /subscribe/{topic}             → register a subscriber, returns subscriber_id
  GET  /consume/{topic}/{subscriber}  → pull next message (returns null if none)
  GET  /topics                        → list topics with queue depth
  GET  /metrics                       → counters since startup
"""

import os
import time
import uuid
import logging
import threading
from collections import defaultdict, deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from falkordb import FalkorDB
from graph_utils import update_and_cascade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mnp")

SERVICE_NAME   = os.getenv("SERVICE_NAME",   "mnp-service")
FALKORDB_HOST  = os.getenv("FALKORDB_HOST",  "localhost")
FALKORDB_PORT  = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_USER  = os.getenv("FALKORDB_USER",  "sampleuser")
FALKORDB_PASS  = os.getenv("FALKORDB_PASS",  "samplePass123")
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "autoops")

MAX_QUEUE_PER_SUBSCRIBER = int(os.getenv("MAX_QUEUE_PER_SUBSCRIBER", "10000"))

_db = None
_graph = None
_FAULT_ACTIVE = False

# In-memory broker state.
#   _subscribers[topic] = { subscriber_id: deque([msg, ...]) }
#   _topic_lock guards reads/writes on _subscribers.
_subscribers: dict = defaultdict(dict)
_topic_lock  = threading.Lock()

_metrics = {
    "messages_published": 0,
    "messages_consumed":  0,
    "subscribers_total":  0,
    "started_at":         time.time(),
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
    logger.info(f"[{SERVICE_NAME}] Started on port 8010")
    yield
    update_and_cascade(get_graph(), SERVICE_NAME, "UNREACHABLE", "Service stopped")
    if _db:
        _db.connection.close()


app = FastAPI(title="AutoOps Messaging & Notification Platform", lifespan=lifespan)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """
    MNP has no upstream service dependency. It depends only on its own
    process state and the graph for reporting. This keeps MNP independent
    so its outage cannot cascade up to notification.
    """
    global _FAULT_ACTIVE
    issues = []

    if _FAULT_ACTIVE:
        issues.append("self: service fault injected")

    graph_ok = get_graph() is not None
    if not graph_ok:
        issues.append("FalkorDB not reachable")

    status  = "UNHEALTHY" if issues else "HEALTHY"
    message = "; ".join(issues) if issues else ""
    update_and_cascade(get_graph(), SERVICE_NAME, status, message)

    return JSONResponse(
        status_code=503 if status == "UNHEALTHY" else 200,
        content={
            "service":     SERVICE_NAME,
            "status":      status,
            "topics":      len(_subscribers),
            "subscribers": sum(len(subs) for subs in _subscribers.values()),
            "issues":      issues,
            "timestamp":   time.time(),
        }
    )


# ── Pub/Sub API ───────────────────────────────────────────────────────────────

@app.post("/publish/{topic}")
async def publish(topic: str, request: Request):
    """
    Publish a message to a topic. Fans out to every subscriber's queue.
    Returns the number of subscribers the message was delivered to.
    """
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    try:
        payload = await request.json()
    except Exception:
        payload = (await request.body()).decode("utf-8", errors="replace")

    message = {
        "message_id":   f"MSG-{uuid.uuid4().hex[:12]}",
        "topic":        topic,
        "payload":      payload,
        "published_at": time.time(),
    }

    delivered = 0
    with _topic_lock:
        subs = _subscribers.get(topic, {})
        for sub_id, queue in subs.items():
            if len(queue) >= MAX_QUEUE_PER_SUBSCRIBER:
                queue.popleft()  # drop oldest (bounded queue)
            queue.append(message)
            delivered += 1

    _metrics["messages_published"] += 1
    logger.info(f"[{SERVICE_NAME}] Published to {topic} → {delivered} subscriber(s)")
    return {
        "message_id":            message["message_id"],
        "topic":                 topic,
        "delivered_subscribers": delivered,
        "published_at":          message["published_at"],
    }


@app.post("/subscribe/{topic}")
def subscribe(topic: str, subscriber_id: str = ""):
    """
    Register a subscriber for a topic. Returns a subscriber_id which the
    caller uses on subsequent /consume calls. If the caller passes their
    own subscriber_id, it is honored (idempotent re-subscribe).
    """
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    sid = subscriber_id or f"SUB-{uuid.uuid4().hex[:12]}"
    with _topic_lock:
        if sid not in _subscribers[topic]:
            _subscribers[topic][sid] = deque(maxlen=MAX_QUEUE_PER_SUBSCRIBER)
            _metrics["subscribers_total"] += 1

    logger.info(f"[{SERVICE_NAME}] Subscriber {sid} registered for topic {topic}")
    return {
        "topic":         topic,
        "subscriber_id": sid,
        "queue_depth":   len(_subscribers[topic][sid]),
    }


@app.get("/consume/{topic}/{subscriber_id}")
def consume(topic: str, subscriber_id: str):
    """
    Pull the next message for this subscriber on this topic.
    Returns the message or `null` if the queue is empty.
    """
    global _FAULT_ACTIVE
    if _FAULT_ACTIVE:
        return JSONResponse(status_code=503, content={"error": "Service fault active"})

    with _topic_lock:
        subs = _subscribers.get(topic, {})
        queue = subs.get(subscriber_id)
        if queue is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Subscriber '{subscriber_id}' not registered on '{topic}'"}
            )
        if not queue:
            return {"message": None, "queue_depth": 0}
        msg = queue.popleft()

    _metrics["messages_consumed"] += 1
    return {"message": msg, "queue_depth": len(queue)}


@app.get("/topics")
def list_topics():
    """List all topics with their subscriber count and total queue depth."""
    with _topic_lock:
        topics = []
        for topic, subs in _subscribers.items():
            total_depth = sum(len(q) for q in subs.values())
            topics.append({
                "topic":           topic,
                "subscriber_count": len(subs),
                "total_queue_depth": total_depth,
                "max_queue_depth":   max((len(q) for q in subs.values()), default=0),
            })
    return {"topics": topics, "count": len(topics)}


# ── Fault injection ───────────────────────────────────────────────────────────

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


@app.get("/metrics")
def metrics():
    uptime = time.time() - _metrics["started_at"]
    with _topic_lock:
        topic_count       = len(_subscribers)
        subscriber_count  = sum(len(subs) for subs in _subscribers.values())
        total_queue_depth = sum(len(q) for subs in _subscribers.values() for q in subs.values())
    return {
        **_metrics,
        "topic_count":       topic_count,
        "subscriber_count":  subscriber_count,
        "total_queue_depth": total_queue_depth,
        "uptime_seconds":    round(uptime, 1),
    }


@app.get("/")
def root():
    return {
        "service":     SERVICE_NAME,
        "version":     "1.0.0",
        "description": "In-memory pub/sub broker for asynchronous event delivery",
    }
