import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import dcr_client as d

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "samples")


def read(name):
    return open(os.path.join(SAMPLES, name)).read()


def test_parse_case_ids():
    ids = d.parse_case_ids(read("sims.xml"))
    assert ids == {"2107443", "2107487", "2107488", "2107489", "2107490"}


def test_parse_case_state():
    s = d.parse_case_state(read("events.json"))
    assert s.accepting is True
    assert [(e.id, e.enabled, e.executed) for e in s.events] == [("A1", True, False), ("A2", False, False)]
    assert [e.id for e in s.executable()] == ["A1"]
    assert s.events[0].label == "Application received"


def test_executed_events_are_not_repeated():
    s = d.parse_case_state(read("events.json"))
    s.events[0].executed = True  # A1 stays enabled but already ran
    assert s.executable() == []


class FakeResp:
    def __init__(self, text=""):
        self.text, self.status_code, self.headers = text, 201, {}


class FakeClient(d.DcrClient):
    """DcrClient with the HTTP layer replaced by an in-memory sim list."""
    def __init__(self, ids, external=()):
        super().__init__("u", "p")
        self.ids, self.external = list(ids), list(external)

    def _request(self, method, path, **kw):
        if method == "POST":
            self.ids.append(str(int(max(self.ids, key=int)) + 1))
            for _ in self.external:  # someone else creates a sim at the same time
                self.ids.append(str(int(max(self.ids, key=int)) + 1))
            return FakeResp()
        body = "<log>" + "".join(f'<trace id="{i}"/>' for i in self.ids) + "</log>"
        return FakeResp(body)


def test_create_case_finds_new_id():
    c = FakeClient(["2107489", "2107490"])
    assert c.create_case("g") == "2107491"
    assert c.create_case("g") == "2107492"


def test_create_case_ambiguous_takes_highest():
    c = FakeClient(["2107490"], external=[1])
    assert c.create_case("g") == "2107492"


def test_mock_walks_the_sample_graph():
    m = d.MockDcrClient()
    cid = m.create_case("g")
    ran = []
    while (todo := m.get_case_state("g", cid).executable()):
        ran.append(todo[0].id)
        m.execute_event("g", cid, todo[0].id)
    assert ran == ["A1", "A2"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
