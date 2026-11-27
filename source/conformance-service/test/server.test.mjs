import assert from "node:assert/strict";
import test from "node:test";
import { checkConformance, createServer } from "../dist/server.js";

const SAMPLE_DCR = `<?xml version="1.0" encoding="UTF-8"?>
<dcr:definitions xmlns:dcr="http://tk/schema/dcr">
  <dcr:dcrGraph id="dcrGraph">
    <dcr:event id="E1" role="Head of Department" description="Open vacancy" included="true" executed="false" pending="false"/>
    <dcr:event id="E2" role="HR" description="Close vacancy" included="true" executed="false" pending="false"/>
    <dcr:relation id="R1" type="condition" sourceRef="E1" targetRef="E2"/>
    <dcr:relation id="R2" type="response" sourceRef="E1" targetRef="E2" time="P30D"/>
  </dcr:dcrGraph>
</dcr:definitions>`;

function xes(traceId, events) {
  const body = events
    .map(
      ([name, role, ts]) =>
        `<event><string key="concept:name" value="${name}"/><string key="role" value="${role}"/><date key="time:timestamp" value="${ts}"/></event>`
    )
    .join("");
  return `<log xes.version="1.0"><trace><string key="concept:name" value="${traceId}"/>${body}</trace></log>`;
}

test("accepts a conforming trace within deadline", () => {
  const res = checkConformance({
    modelXml: SAMPLE_DCR,
    logXml: xes("VAC-1", [
      ["Open vacancy", "Head of Department", "2026-01-01T09:00:00.000Z"],
      ["Close vacancy", "HR", "2026-01-10T09:00:00.000Z"],
    ]),
  });
  assert.equal(res.conformant, true);
  assert.equal(res.conforming, 1);
  assert.equal(res.totalStructuralViolations, 0);
  assert.equal(res.totalTimeViolations, 0);
});

test("flags condition, role, pending, and P30D time violations", () => {
  const outOfOrder = checkConformance({
    modelXml: SAMPLE_DCR,
    logXml: xes("VAC-2", [["Close vacancy", "HR", "2026-01-01T09:00:00.000Z"]]),
  });
  assert.equal(outOfOrder.conformant, false);
  assert.ok(outOfOrder.totalStructuralViolations > 0);

  const wrongRole = checkConformance({
    modelXml: SAMPLE_DCR,
    logXml: xes("VAC-3", [
      ["Open vacancy", "HR", "2026-01-01T09:00:00.000Z"],
      ["Close vacancy", "HR", "2026-01-05T09:00:00.000Z"],
    ]),
  });
  assert.equal(wrongRole.conformant, false);

  const unclosed = checkConformance({
    modelXml: SAMPLE_DCR,
    logXml: xes("VAC-4", [["Open vacancy", "Head of Department", "2026-01-01T09:00:00.000Z"]]),
  });
  assert.equal(unclosed.conformant, false);
  assert.equal(unclosed.violating, 1);

  const late = checkConformance({
    modelXml: SAMPLE_DCR,
    logXml: xes("VAC-5", [
      ["Open vacancy", "Head of Department", "2026-01-01T09:00:00.000Z"],
      ["Close vacancy", "HR", "2026-03-01T09:00:00.000Z"],
    ]),
  });
  assert.equal(late.conformant, false);
  assert.ok(late.totalTimeViolations > 0);
});

test("HTTP server serves /health and /check", async (t) => {
  const server = createServer();
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(() => server.close());
  const url = `http://127.0.0.1:${server.address().port}`;

  assert.equal((await fetch(`${url}/health`)).status, 200);
  const res = await fetch(`${url}/check`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      modelXml: SAMPLE_DCR,
      logXml: xes("VAC-HTTP", [
        ["Open vacancy", "Head of Department", "2026-01-01T09:00:00.000Z"],
        ["Close vacancy", "HR", "2026-01-05T09:00:00.000Z"],
      ]),
    }),
  });
  assert.equal(res.status, 200);
  assert.equal((await res.json()).conformant, true);
});
