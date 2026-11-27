import http from "node:http";
import { parseRoleLog } from "./dcr-engine/eventLogs";
import { quantifyViolations, replayTraceS } from "./dcr-engine/conformance";
import { copyMarking } from "./dcr-engine/utility";
import { parseDcrEditorXml } from "./xmlToDcr";

export interface CheckRequest {
  modelXml: string;
  logXml: string;
  modelName?: string;
  logName?: string;
}

export interface CheckResponse {
  modelName: string;
  logName: string;
  conforming: number;
  partiallyViolating: number;
  violating: number;
  totalStructuralViolations: number;
  totalTimeViolations: number;
  conformant: boolean;
  details: string;
}

export function checkConformance(req: CheckRequest): CheckResponse {
  if (!req.modelXml || !req.modelXml.trim()) {
    throw new Error("modelXml is required");
  }
  if (!req.logXml || !req.logXml.trim()) {
    throw new Error("logXml is required");
  }

  const { graph, hasSubProcesses } = parseDcrEditorXml(req.modelXml);
  const log = parseRoleLog(req.logXml);
  const initialMarking = copyMarking(graph.marking);

  let conforming = 0;
  let partiallyViolating = 0;
  let violating = 0;
  let totalStructuralViolations = 0;
  let totalTimeViolations = 0;
  const traceSummaries: string[] = [];

  for (const [traceId, trace] of Object.entries(log.traces)) {
    graph.marking = copyMarking(initialMarking);
    const structurallyPositive = replayTraceS(graph, trace, graph.initialVariableStore ?? {});

    graph.marking = copyMarking(initialMarking);
    const quant = !hasSubProcesses
      ? quantifyViolations(graph, trace, graph.initialVariableStore ?? {})
      : undefined;

    const structural = quant?.totalViolations ?? 0;
    const temporal = quant?.totalTimeViolations ?? 0;
    const isPositive = structurallyPositive && temporal === 0;

    let status: string;
    if (isPositive) {
      conforming++;
      status = "Conforming";
    } else if (quant?.finalStateAccepting) {
      partiallyViolating++;
      status = "Partially Violating";
    } else {
      violating++;
      status = "Violating";
    }

    totalStructuralViolations += structural;
    totalTimeViolations += temporal;
    traceSummaries.push(
      `Trace ${traceId}: ${status} (events=${trace.length}, structuralViolations=${structural}, timeViolations=${temporal})`
    );
  }

  const conformant =
    violating === 0 &&
    partiallyViolating === 0 &&
    totalStructuralViolations === 0 &&
    totalTimeViolations === 0;

  return {
    modelName: req.modelName || "model.dcr.xml",
    logName: req.logName || "trace.xes",
    conforming,
    partiallyViolating,
    violating,
    totalStructuralViolations,
    totalTimeViolations,
    conformant,
    details: traceSummaries.join("\n"),
  };
}

function readJsonBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf-8")));
      } catch (err) {
        reject(new Error("Invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown): void {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

if (require.main === module) {
  const port = Number(process.env.PORT || 8091);
  const host = process.env.HOST || "0.0.0.0";

  const server = http.createServer(async (req, res) => {
    if (req.method === "GET" && req.url === "/health") {
      return sendJson(res, 200, { status: "ok", engine: "dcr-engine" });
    }
    if (req.method === "POST" && req.url === "/check") {
      try {
        const body = await readJsonBody(req);
        const result = checkConformance(body);
        return sendJson(res, 200, result);
      } catch (err: any) {
        return sendJson(res, 400, { error: err?.message || "Conformance check failed" });
      }
    }
    return sendJson(res, 404, { error: "Not found" });
  });

  server.listen(port, host, () => {
    console.log(`DCR conformance service (dcr-engine) listening on http://${host}:${port}`);
  });
}
