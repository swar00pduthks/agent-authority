"""Human approvals: close the chain of authority at a named person.

An agent can *open* a pull request, but a person approves and merges it. Those decisions arrive from GitHub or
GitLab webhooks (or straight from the host app) and become OpenLineage runs of kind ``approval`` under the
agent session that produced the change, so the chain of authority ends at the human who let it ship.

Linking a pull request to its session: the app builder writes a marker into the PR description,

    agent-authority-session: <session root run id>

which the webhook parsers look for. The webhook usually arrives after the session has finished; that is fine,
the approval run simply joins the session's tree (and its integrity chain) later.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from agent_authority.sdk import PRODUCER, SCHEMA_URL

SESSION_MARKER = re.compile(r"agent-authority-session:\s*([0-9a-fA-F-]{36})")
APPROVED = {"approved", "merged"}


@dataclass
class ApprovalDecision:
    session_id: str
    approver: str  # e.g. "user:priya.sharma" or "github:octocat"
    decision: str  # approved | merged | changes_requested | rejected
    provider: str = "api"  # github | gitlab | api
    url: str | None = None
    reference: str | None = None  # e.g. "PR #42"
    repository: tuple[str, str] | None = None  # (namespace, name) of the repo dataset
    at: datetime = field(default_factory=lambda: datetime.now(UTC))


class WebhookError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status, self.message = status, message


# ----------------------------------------------------------------------------- events
def approval_events(session: Any, d: ApprovalDecision) -> list[dict[str, Any]]:
    """OpenLineage START + COMPLETE events for one approval, parented to the session's root run."""
    run_id = str(uuid.uuid4())
    t = d.at.astimezone(UTC).isoformat()
    name = f"approval.{d.provider}"
    facet = {
        "_producer": PRODUCER,
        "_schemaURL": "https://github.com/swar00pduthks/agent-authority/spec/AgentRunFacet.json",
        "kind": "approval",
        "onBehalfOf": d.approver,
        "policy": {"decision": d.decision, "rule": f"{d.provider}-review"},
        "approval": {k: v for k, v in {"provider": d.provider, "url": d.url, "reference": d.reference}.items() if v},
    }
    parent = {
        "_producer": PRODUCER,
        "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json",
        "run": {"runId": session.run_id},
        "job": {"namespace": session.namespace, "name": session.job_name},
        "root": {"run": {"runId": session.run_id}, "job": {"namespace": session.namespace, "name": session.job_name}},
    }
    outputs = [{"namespace": d.repository[0], "name": d.repository[1], "facets": {}}] if d.repository else []
    base = {
        "producer": PRODUCER, "schemaURL": SCHEMA_URL,
        "job": {"namespace": session.namespace, "name": name, "facets": {}},
        "inputs": [],
    }
    return [
        {**base, "eventType": "START", "eventTime": t, "outputs": [],
         "run": {"runId": run_id, "facets": {"agent": facet, "parent": parent}}},
        {**base, "eventType": "COMPLETE", "eventTime": t, "outputs": outputs,
         "run": {"runId": run_id, "facets": {"agent": facet, "parent": parent}}},
    ]


# --------------------------------------------------------------------------- webhooks
def verify_github(secret: str | None, body: bytes, signature: str | None) -> None:
    if not secret:
        return
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not signature or not hmac.compare_digest(expected, signature):
        raise WebhookError(401, "invalid GitHub webhook signature")


def verify_gitlab(secret: str | None, token: str | None) -> None:
    if secret and not (token and hmac.compare_digest(secret, token)):
        raise WebhookError(401, "invalid GitLab webhook token")


def _session_id(*texts: str | None) -> str | None:
    for t in texts:
        if t and (m := SESSION_MARKER.search(t)):
            return m.group(1).lower()
    return None


def parse_github(event: str, payload: dict[str, Any]) -> ApprovalDecision | None:
    """`pull_request_review` (state approved / changes_requested) and `pull_request` closed + merged."""
    pr = payload.get("pull_request") or {}
    sid = _session_id(pr.get("body"), pr.get("title"))
    if not sid:
        return None
    repo = payload.get("repository") or {}
    repository = ("git://github.com/" + (repo.get("owner") or {}).get("login", ""), repo.get("name", "")) if repo else None
    ref = f"PR #{pr.get('number')}" if pr.get("number") else None
    if event == "pull_request_review" and payload.get("action") == "submitted":
        review = payload.get("review") or {}
        state = (review.get("state") or "").lower()
        if state not in ("approved", "changes_requested"):
            return None
        return ApprovalDecision(sid, f"github:{(review.get('user') or {}).get('login', 'unknown')}", state, "github",
                                review.get("html_url") or pr.get("html_url"), ref, repository,
                                _ts(review.get("submitted_at")))
    if event == "pull_request" and payload.get("action") == "closed" and pr.get("merged"):
        return ApprovalDecision(sid, f"github:{(pr.get('merged_by') or {}).get('login', 'unknown')}", "merged", "github",
                                pr.get("html_url"), ref, repository, _ts(pr.get("merged_at")))
    return None


def parse_gitlab(payload: dict[str, Any]) -> ApprovalDecision | None:
    """Merge request hooks: `approved`, `unapproved` and `merge` actions."""
    if payload.get("object_kind") != "merge_request":
        return None
    mr = payload.get("object_attributes") or {}
    sid = _session_id(mr.get("description"), mr.get("title"))
    action = mr.get("action")
    decision = {"approved": "approved", "merge": "merged", "unapproved": "rejected"}.get(action or "")
    if not sid or not decision:
        return None
    proj = payload.get("project") or {}
    host = (proj.get("web_url") or "https://gitlab.com").split("/")[2] if proj.get("web_url") else "gitlab.com"
    ns, _, name = (proj.get("path_with_namespace") or "/").rpartition("/")
    user = (payload.get("user") or {}).get("username", "unknown")
    return ApprovalDecision(sid, f"gitlab:{user}", decision, "gitlab", mr.get("url"),
                            f"MR !{mr.get('iid')}" if mr.get("iid") else None,
                            (f"git://{host}/{ns}", name) if name else None, _ts(mr.get("updated_at")))


def _ts(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" UTC", "+00:00"))
    except ValueError:
        return datetime.now(UTC)
