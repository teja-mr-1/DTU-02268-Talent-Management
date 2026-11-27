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

## 4. Manager demo

With the applications service running, open `http://localhost:8090/manager.html`.
The overview shows six hardcoded pending applications and counts by position.
“Distribute applications” opens `/distribution.html`, where those same applications
have fixed employee reviewers. “Confirm distribution” sends the proposed assignments.

The overview calls `GET /applications?status=pending`, executing DCR event `A1`
and returning an empty JSON array. Confirmation calls `PUT /applications/distribute`,
executing DCR event `A12` and returning HTTP 204. The UI still uses shared sample data.
The existing application form remains at `/index.html`.

## 5. DCR scale-up process

Set `DCR_USERNAME` and `DCR_PASSWORD` in your environment or a local Compose `.env`
file before running `docker compose up --build -d`. Spring reads these through
`dcr.username` and `dcr.password` in `application.properties`; every DCR request
uses Basic Auth. `dcr.base-url` points to graph `2016808`.

Crossing the rolling-hour threshold (more than five applications) creates a DCR
simulation, lists simulations, and stores the newest ID by its `created` timestamp.
An active process prevents another scale-up. State is shared in memory between
the embedded Flink job and HTTP endpoints and lasts until the application restarts;
there is currently no completion/reset flow. Manager requests return HTTP 409 when
no simulation ID is available and HTTP 502 when DCR rejects or cannot receive an event.

The application-received process continues to use Camunda.

## 6. Step 3: Trace Collection & DCR Conformance Checking

Hiring trace events are sent into the `applications` service via REST (for example using Camunda 8's Outbound REST Connector), grouped per `vacancyId` in embedded Flink (`VacancyTraceCollector`), written as an IEEE `.xes` file when `terminal: true` arrives (`XesLogBuilder`), and checked via HTTP against the lightweight `conformance` sidecar (`source/conformance-service`) powered directly by DTU Process Doctor's `dcr-engine` (`hugoalopez-dtu/dcr-js`). Generated `.xes` logs can also be downloaded and uploaded directly into `https://processdoctor.compute.dtu.dk/` for visual heatmap inspection.

### Endpoints

- `POST /api/traces/events` - Send a hiring trace event into Flink:
  ```json
  {
    "vacancyId": "VAC-FP-2026-01",
    "eventName": "Confirm funding availability",
    "role": "Head of Finance",
    "timestamp": "2026-11-26T12:00:00Z",
    "terminal": false
  }
  ```
  `role` (defaults to `"System"`), `timestamp` (ISO-8601, defaults to now), and `terminal` (defaults to `false`) are optional. Set `"terminal": true` on the final event to publish the `.xes` log and trigger conformance checking.
- `GET /api/traces` - View collected traces, `.xes` paths, and conformance verdicts.
- `GET /api/traces/{vacancyId}/xes` - Download the generated `.xes` XML file.

### Configuration

- `XES_OUTPUT_DIR` - Output directory for `.xes` files (default: `data/xes`, mapped to `./xes-logs` in Docker Compose).
- `AUTO_CONFORMANCE_CHECK` - `true` (default) to check automatically via the `conformance` service when a terminal event arrives, or `false` to only write `.xes` files.
- `CONFORMANCE_SERVICE_URL` - URL of the `dcr-engine` conformance service (default: `http://127.0.0.1:8091`, set to `http://conformance:8091` in `compose.yaml`).
- `DCR_MODEL_PATH` - Optional path to override the bundled default `Full_Professor_Hiring_Compliance.dcr.xml`.
