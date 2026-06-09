"""
graph_utils.py  –  AutoOps PoC shared module
=============================================
Drop this file into every service folder alongside main.py.
Update each service's Dockerfile to also COPY graph_utils.py.

Single runtime field used everywhere: `status`
  - Seeded as 'HEALTHY' by the RCA agent (ON CREATE SET n.status = 'HEALTHY')
  - Updated at runtime exclusively through update_and_cascade()
  - Never a second field.  `state` is REMOVED.

Responsibilities
────────────────
1. update_service_state(g, name, status, message)
   - SET the :Service node's status + lastUpdated + message

2. cascade_downstream(g, source_name, new_status)
   - Walk DEPENDS_ON in reverse (who depends ON source?)
   - If source went UNHEALTHY/UNREACHABLE:
       mark direct + transitive dependents DEGRADED
       (never overwrite a service's own self-reported UNHEALTHY)
   - If source recovered to HEALTHY:
       restore DEGRADED dependents only when ALL their
       upstream dependencies are also HEALTHY

3. manage_jira_ticket(g, service_name, new_status, message)
   - UNHEALTHY transition → create an OPEN :JiraTicket if none exists
   - HEALTHY   transition → RESOLVE any OPEN ticket for this service

The calling service only needs:
    from graph_utils import update_and_cascade

Replace every  update_falkordb_state(state, msg)  call with:
    update_and_cascade(graph, SERVICE_NAME, status, msg)
"""

import time
import logging
from datetime import datetime, timezone

logger = logging.getLogger("graph_utils")

UNHEALTHY_STATES = {"UNHEALTHY", "UNREACHABLE"}


# ── 1. Single-node status update ──────────────────────────────────────────────

def update_service_state(g, name: str, status: str, message: str = "") -> None:
    """
    Write the single runtime field `status` (plus lastUpdated and message)
    onto the named :Service (or :MQTTBroker / :Datastore) node.

    No secondary `state` field is created or touched.
    """
    try:
        g.query(
            """
            MATCH (s {name: $name})
            SET s.status      = $status,
                s.lastUpdated = timestamp(),
                s.message     = $message
            """,
            {"name": name, "status": status, "message": message},
        )
        logger.debug("[graph_utils] %s → status=%s", name, status)
    except Exception as exc:
        logger.warning(
            "[graph_utils] update_service_state failed for %s: %s", name, exc
        )


# ── 2. Cascade ────────────────────────────────────────────────────────────────

def _get_all_dependents(g, source_name: str) -> list:
    """
    Return names of ALL services that transitively depend on source_name
    via DEPENDS_ON (any depth).
    """
    try:
        result = g.query(
            """
            MATCH (downstream:Service)-[:DEPENDS_ON*1..]->(source {name: $name})
            RETURN DISTINCT downstream.name AS svc
            """,
            {"name": source_name},
        )
        return [row[0] for row in result.result_set if row[0]]
    except Exception as exc:
        logger.warning("[graph_utils] _get_all_dependents failed: %s", exc)
        return []


def _all_upstreams_healthy(g, service_name: str) -> bool:
    """
    Return True only when every direct upstream dependency of service_name
    reports `status = 'HEALTHY'` in the graph.
    """
    try:
        result = g.query(
            """
            MATCH (s:Service {name: $name})-[:DEPENDS_ON]->(dep)
            WHERE dep.status IN ['UNHEALTHY', 'UNREACHABLE', 'DEGRADED']
            RETURN count(dep) AS cnt
            """,
            {"name": service_name},
        )
        cnt = result.result_set[0][0] if result.result_set else 0
        return cnt == 0
    except Exception as exc:
        logger.warning(
            "[graph_utils] _all_upstreams_healthy failed for %s: %s", service_name, exc
        )
        return False


def cascade_downstream(g, source_name: str, new_status: str) -> None:
    """
    Propagate source_name's new status to all downstream services.

    UNHEALTHY / UNREACHABLE
        → mark every transitive dependent as DEGRADED
          (only if they are currently HEALTHY — never overwrite a service's
           own self-reported UNHEALTHY)

    HEALTHY
        → restore DEGRADED dependents whose *all* direct upstreams are
          also HEALTHY
    """
    if new_status in UNHEALTHY_STATES:
        dependents = _get_all_dependents(g, source_name)
        if not dependents:
            return

        logger.info(
            "[graph_utils] cascade UNHEALTHY from '%s' → %d downstream(s)",
            source_name, len(dependents),
        )
        for svc in dependents:
            try:
                g.query(
                    """
                    MATCH (s:Service {name: $name})
                    WHERE s.status = 'HEALTHY'
                    SET s.status      = 'DEGRADED',
                        s.lastUpdated = timestamp(),
                        s.message     = $msg
                    """,
                    {
                        "name": svc,
                        "msg":  f"Upstream '{source_name}' is {new_status}",
                    },
                )
                logger.info(
                    "[graph_utils] %s → DEGRADED (upstream: %s)", svc, source_name
                )
            except Exception as exc:
                logger.warning(
                    "[graph_utils] degrade failed for %s: %s", svc, exc
                )

    elif new_status == "HEALTHY":
        dependents = _get_all_dependents(g, source_name)
        if not dependents:
            return

        logger.info(
            "[graph_utils] cascade RECOVERY from '%s' → checking %d downstream(s)",
            source_name, len(dependents),
        )
        for svc in dependents:
            if _all_upstreams_healthy(g, svc):
                try:
                    g.query(
                        """
                        MATCH (s:Service {name: $name})
                        WHERE s.status = 'DEGRADED'
                        SET s.status      = 'HEALTHY',
                            s.lastUpdated = timestamp(),
                            s.message     = 'All upstream dependencies recovered'
                        """,
                        {"name": svc},
                    )
                    logger.info(
                        "[graph_utils] %s → HEALTHY (all upstreams ok)", svc
                    )
                except Exception as exc:
                    logger.warning(
                        "[graph_utils] recovery failed for %s: %s", svc, exc
                    )


# ── 3. Jira ticket management ─────────────────────────────────────────────────

def _make_ticket_id(service_name: str) -> str:
    date_str  = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    safe_name = service_name.replace("-", "_").upper()[:20]
    return f"INC-{safe_name}-{date_str}"


def manage_jira_ticket(
    g, service_name: str, new_status: str, message: str = ""
) -> None:
    """
    Open a :JiraTicket when the service becomes UNHEALTHY (idempotent).
    Resolve it automatically when the service returns to HEALTHY.
    """
    if new_status in UNHEALTHY_STATES:
        _open_ticket_if_needed(g, service_name, new_status, message)
    elif new_status == "HEALTHY":
        _resolve_open_ticket(g, service_name)


def _open_ticket_if_needed(
    g, service_name: str, status: str, message: str
) -> None:
    try:
        existing = g.query(
            """
            MATCH (t:JiraTicket)-[:AFFECTED]->(s {name: $svc})
            WHERE t.status = 'OPEN'
            RETURN t.name LIMIT 1
            """,
            {"svc": service_name},
        )
        if existing.result_set:
            return  # already an open ticket — idempotent

        ticket_id = _make_ticket_id(service_name)
        now_ms    = int(time.time() * 1000)
        summary   = (
            f"[AUTO] {service_name} is {status}"
            + (f": {message[:100]}" if message else "")
        )
        g.query(
            """
            MERGE (t:JiraTicket {name: $tid})
            SET t.summary          = $summary,
                t.affected_service = $svc,
                t.root_cause_node  = $svc,
                t.failure_mode     = $status,
                t.severity         = 'HIGH',
                t.status           = 'OPEN',
                t.occurred_at      = $ts,
                t.resolution_notes = ''
            WITH t
            MATCH (s {name: $svc})
            MERGE (t)-[:AFFECTED]->(s)
            """,
            {
                "tid":     ticket_id,
                "summary": summary,
                "svc":     service_name,
                "status":  status,
                "ts":      now_ms,
            },
        )
        logger.info(
            "[graph_utils] Opened ticket %s for %s", ticket_id, service_name
        )
    except Exception as exc:
        logger.warning(
            "[graph_utils] _open_ticket_if_needed failed for %s: %s",
            service_name, exc,
        )


def _resolve_open_ticket(g, service_name: str) -> None:
    try:
        now_ms = int(time.time() * 1000)
        g.query(
            """
            MATCH (t:JiraTicket)-[:AFFECTED]->(s {name: $svc})
            WHERE t.status = 'OPEN'
            SET t.status           = 'RESOLVED',
                t.resolved_at      = $ts,
                t.resolution_notes = 'Service recovered automatically'
            """,
            {"svc": service_name, "ts": now_ms},
        )
        logger.info(
            "[graph_utils] Resolved open ticket(s) for %s", service_name
        )
    except Exception as exc:
        logger.warning(
            "[graph_utils] _resolve_open_ticket failed for %s: %s",
            service_name, exc,
        )


# ── Public entry point ────────────────────────────────────────────────────────

def update_and_cascade(
    g, service_name: str, status: str, message: str = ""
) -> None:
    """
    All-in-one replacement for each service's inline update_falkordb_state().

    Uses the single field `status` — no secondary `state` field is ever created.

    1. Updates this service's own node:  status, lastUpdated, message
    2. Cascades the change (DEGRADED or HEALTHY) to downstream dependents
    3. Opens / resolves a :JiraTicket for this service

    Replace every call to update_falkordb_state(state, msg) with:
        update_and_cascade(graph, SERVICE_NAME, status, msg)
    """
    if g is None:
        return
    update_service_state(g, service_name, status, message)
    cascade_downstream(g, service_name, status)
    manage_jira_ticket(g, service_name, status, message)