"""DCR Graphs access layer (adapted from dcr_helper.py, shaped around real API responses).

What the real API looks like:
  * POST  /graphs/{g}/sims                 -> 201, EMPTY body (no case id returned)
  * GET   /graphs/{g}/sims?filter=all      -> plain XML: <log><trace id="..."/>...</log>
  * GET   /graphs/{g}/sims/{c}/events      -> the XML wrapped in a JSON string; lists ALL events
                                              with enabled / executed flags, plus isAccepting
  * POST  /graphs/{g}/sims/{c}/events/{e}  -> executes an event

Changes from the original helper: parameters instead of import-time env, a shared session
with timeouts, real XML parsing instead of regexes, and errors RAISE instead of returning
[] (so "API failed" can't look like "case finished").
DCR_MODE=mock (the default) runs a fake case shaped like the sample graph, no account needed.
"""
import json
import logging
import os
import threading
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import requests

log = logging.getLogger("dcr")

API_BASE = os.getenv("DCR_API_BASE", "https://repository.dcrgraphs.net/api")
TIMEOUT = 10


class DcrError(Exception):
    pass


# ---------- parsing (pure functions, tested against ./samples) ----------
@dataclass
class DcrEvent:
    id: str
    label: str
    enabled: bool
    executed: bool
    sequence: int


@dataclass
class CaseState:
    events: list = field(default_factory=list)
    accepting: bool = False

    def executable(self) -> list:
        """Enabled events not yet executed, in graph order. We skip already-executed events
        so a repeatable event (one that stays enabled) can't make us loop forever."""
        return sorted((e for e in self.events if e.enabled and not e.executed),
                      key=lambda e: e.sequence)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _flag(el, name: str) -> bool:
    return (el.get(name) or "").strip().lower() == "true"


def _parse(text: str):
    """The events endpoint returns XML as a JSON-encoded string ("<events ...>" with \\" escapes);
    the sims endpoint returns plain XML. Handle both."""
    try:
        decoded = json.loads(text)
        if isinstance(decoded, str):
            text = decoded
    except ValueError:
        pass
    try:
        return ET.fromstring(text)
    except ET.ParseError as e:
        raise DcrError(f"Unparseable DCR response: {e}: {text[:200]}") from e


def parse_case_ids(text: str) -> set:
    root = _parse(text)
    return {el.get("id") for el in root.iter() if _local(el.tag) == "trace" and el.get("id")}


def parse_case_state(text: str) -> CaseState:
    root = _parse(text)
    events = []
    for el in root.iter():
        if _local(el.tag) != "event":
            continue
        try:
            seq = int(el.get("sequence") or 0)
        except ValueError:
            seq = 0
        events.append(DcrEvent(
            id=el.get("id", ""), label=el.get("label", ""),
            enabled=_flag(el, "enabled"), executed=_flag(el, "executed"), sequence=seq,
        ))
    return CaseState(events=events, accepting=_flag(root, "isAccepting"))


# ---------- real client ----------
class DcrClient:
    def __init__(self, username, password, api_base=API_BASE):
        if not username or not password:
            raise DcrError("DCR_USERNAME and DCR_PASSWORD must be set when DCR_MODE=real")
        self.api_base = api_base
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"Accept": "application/xml"})
        self._lock = threading.Lock()
        self._known = {}  # graph_id -> set of case ids we have already seen

    def _request(self, method, path, **kwargs):
        r = self.session.request(method, f"{self.api_base}{path}", timeout=TIMEOUT, **kwargs)
        if r.status_code not in (200, 201, 204):
            raise DcrError(f"{method} {path} -> {r.status_code}: {r.text[:200]}")
        return r

    def _list_case_ids(self, graph_id) -> set:
        return parse_case_ids(self._request("GET", f"/graphs/{graph_id}/sims?filter=all").text)

    def create_case(self, graph_id: str) -> str:
        """The create call returns no id, so we find our case by diffing the sim list
        before/after. The lock makes this safe within one process (keep the creating
        operator at parallelism 1). Sims created by someone else in the same window
        (e.g. in the DCR UI) make the diff ambiguous; we then take the highest id."""
        if not graph_id:
            raise DcrError("No DCR graph id configured (DCR_GRAPH_ID / DCR_GRAPH_IDS)")
        with self._lock:
            if graph_id not in self._known:
                self._known[graph_id] = self._list_case_ids(graph_id)
            self._request("POST", f"/graphs/{graph_id}/sims", json={})
            after = self._list_case_ids(graph_id)
            new = after - self._known[graph_id]
            self._known[graph_id] = after
            if not new:
                raise DcrError("Sim was created but no new case id appeared in the list")
            if len(new) > 1:
                log.warning("Ambiguous create: %d new sims appeared, using the highest id", len(new))
            return str(max(new, key=int))

    def get_case_state(self, graph_id: str, case_id: str) -> CaseState:
        r = self._request("GET", f"/graphs/{graph_id}/sims/{case_id}/events")
        return parse_case_state(r.text)

    def execute_event(self, graph_id: str, case_id: str, event_id: str) -> None:
        self._request("POST", f"/graphs/{graph_id}/sims/{case_id}/events/{event_id}", json={})


# ---------- mock client ----------
class MockDcrClient:
    """Fake case shaped like the sample graph: A1 'Application received' -> A2 'Application
    processed'. State lives in-process, which is fine: only the worker steps cases."""
    GRAPH = [("A1", "Application received"), ("A2", "Application processed")]

    def __init__(self):
        self._executed = {}

    def create_case(self, graph_id):
        case_id = str(uuid.uuid4())
        log.warning("[MOCK DCR] created case %s", case_id)
        return case_id

    def get_case_state(self, graph_id, case_id):
        n = self._executed.get(case_id, 0)
        events = [DcrEvent(id=eid, label=label, enabled=(i == n), executed=(i < n), sequence=i + 1)
                  for i, (eid, label) in enumerate(self.GRAPH)]
        return CaseState(events=events, accepting=True)

    def execute_event(self, graph_id, case_id, event_id):
        self._executed[case_id] = self._executed.get(case_id, 0) + 1


def get_client():
    if os.getenv("DCR_MODE", "mock") == "real":
        return DcrClient(os.getenv("DCR_USERNAME"), os.getenv("DCR_PASSWORD"))
    return MockDcrClient()
