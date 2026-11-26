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

Hiring trace events are sent into the `applications` service via REST (for example using Camunda 8's Outbound REST Connector), grouped per `vacancyId` in embedded Flink (`VacancyTraceCollector`), written as an IEEE `.xes` file when `terminal: true` arrives (`XesLogBuilder`), and checked against the DCR compliance model on DTU Process Doctor (`ConformanceChecker`).

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
  `role` (defaults to `"System"`), `timestamp` (ISO-8601, defaults to now), and `terminal` (defaults to `false`) are optional. Set `"terminal": true` on the final event to publish the `.xes` log and run conformance checking.
- `GET /api/traces` - View collected traces, `.xes` paths, and Process Doctor conformance verdicts.
- `GET /api/traces/{vacancyId}/xes` - Download the generated `.xes` XML file.

### Configuration

- `XES_OUTPUT_DIR` - Output directory for `.xes` files (default: `data/xes`, mapped to `./xes-logs` in Docker Compose).
- `AUTO_CONFORMANCE_CHECK` - `true` (default) to check automatically on Process Doctor when a terminal event arrives, or `false` to only write `.xes` files.
- `DCR_MODEL_PATH` - Optional path to override the bundled default `Full_Professor_Hiring_Compliance.dcr.xml`.
