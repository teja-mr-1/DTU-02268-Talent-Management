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

## 6. DCR conformance checking

The `conformance` service (`source/conformance-service`, port `8091`) runs DTU Process
Doctor's `dcr-engine` (`hugoalopez-dtu/dcr-js`, fetched at build time) over HTTP
(`POST /check`, `GET /health`).

In `source/flink-app`, inject `ConformanceChecker` and call `check(modelFile, logFile)`
or `checkXml(modelXml, modelName, logXml, logName)`. The service URL comes from
`CONFORMANCE_SERVICE_URL` (set in `compose.yaml`).
Each XES event's `concept:name` and `role` attributes must match the DCR model's
`description` and `role` attributes.

Run tests with `cd source/conformance-service && npm test` and `cd source/flink-app && ./gradlew test`.
