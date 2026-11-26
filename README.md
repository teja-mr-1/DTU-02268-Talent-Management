# DTU Compute Talent Management System

**Course:** 02268 - Process-Oriented and Event-Driven Software Systems  
**Institution:** Technical University of Denmark (DTU)  

---

## 1. Overview
This repository contains the process models, event-processing components, and documentation for the DTU Compute Talent Management project.

---

## 2. Skeleton Structure

```text
DTU-02268-Talent-Management/
├── models/
│   ├── bpmn/           # Camunda BPMN models
│   ├── dcr/            # DCR graphs
│   └── dmn/            # DMN decision tables
├── cep-siddhi/         # Complex Event Processing (Siddhi)
├── presentation/       # Presentation slides
├── docs/               # Course project specifications
├── .gitignore
└── README.md
```

---

## 3. Modeling Tools & Links

* **Camunda Modeler (BPMN & DMN):** [https://camunda.com/download/modeler/](https://camunda.com/download/modeler/)
* **Process Doctor (DTU DCR Tool):** [https://processdoctor.compute.dtu.dk/](https://processdoctor.compute.dtu.dk/)
* **DCR Portal:** [https://www.dcrgraphs.net/](https://www.dcrgraphs.net/)
* **Siddhi CEP Engine:** [https://siddhi.io/](https://siddhi.io/)

---

## 4. Step 3: Trace Collection & DCR Conformance Checking

The `applications` service collects hiring trace events per `vacancyId` in embedded Apache Flink (`VacancyTraceCollector`), writes an IEEE `.xes` log once a terminal event arrives (`XesLogBuilder`), and verifies compliance against the DCR model on DTU Process Doctor (`ConformanceChecker` / `ConformanceCheckSink`).

### Connecting from Camunda 8 (Outbound REST Connector)

Configure a Camunda 8 Outbound REST Connector pointing to `http://applications:8090/api/traces/events` (inside Docker Compose) or `http://localhost:8090/api/traces/events` (locally):

- **Push one event per completed activity (`POST /api/traces/events`):**
  ```json
  {
    "vacancyId": "VAC-FP-2026-01",
    "eventName": "Confirm funding availability",
    "role": "Head of Finance",
    "timestamp": "2026-11-26T12:00:00Z",
    "terminal": false
  }
  ```
  `role` (defaults to `"System"`), `timestamp` (ISO-8601 or epoch ms, defaults to now), and `terminal` (defaults to `false`) are optional. Send `"terminal": true` on the final process activity to publish the `.xes` log and trigger conformance checking.

- **Or send the full trace on process completion (`POST /api/traces/batch`):**
  ```json
  {
    "vacancyId": "VAC-FP-2026-01",
    "events": [
      { "eventName": "Confirm funding availability", "role": "Head of Finance" },
      { "eventName": "Close position unfilled", "role": "HR" }
    ]
  }
  ```

### Step 3 Endpoints

- `POST /api/traces/events` - Enqueue a single hiring trace event into Flink.
- `POST /api/traces/batch` - Enqueue an ordered list of events for a vacancy (final event marked `terminal=true` by default).
- `GET /api/traces` - List all tracked vacancies, collected events, XES paths, and Process Doctor verdicts.
- `GET /api/traces/{vacancyId}` - Inspect trace state and conformance result for one vacancy.
- `GET /api/traces/{vacancyId}/xes` - Download the generated IEEE `.xes` XML log.
- `POST /api/traces/{vacancyId}/check` - Run or re-run Process Doctor DCR conformance checking for a published vacancy trace (accepts optional `{"modelXml": "..."}`).
- `GET /api/conformance` & `POST /api/conformance/check` - Direct conformance checking API (supports JSON, raw XML, or multipart `.xes` + optional `.dcr.xml` model).

### Configuration

- `XES_OUTPUT_DIR` - Directory for generated `.xes` files (default: `data/xes`, `/data/xes` in Docker Compose mapped to `./xes-logs`).
- `AUTO_CONFORMANCE_CHECK` - Set to `true` (default) to automatically run Process Doctor when a terminal event arrives, or `false` to only generate `.xes` logs for manual checking.
- `DCR_MODEL_PATH` - Optional path to a custom DCR XML compliance model (overrides the bundled default `Full_Professor_Hiring_Compliance.dcr.xml`).
- `PROCESS_DOCTOR_URL` - Target Process Doctor URL (default: `https://processdoctor.compute.dtu.dk/`).
