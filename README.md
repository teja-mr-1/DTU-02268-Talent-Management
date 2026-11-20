# DTU Compute Talent Management System

**Course:** 02268 - Process-Oriented and Event-Driven Software Systems  
**Institution:** Technical University of Denmark (DTU)

This branch (`teja`) contains the end-to-end **Full Professor Hiring** PAIS + CEP + Conformance Checking pipeline:

1. **Step 1 - Applicant Portal & Flink CEP (`source/application-form`, `RollingHour`):**
   - Candidate form styled after DTU's Oracle HCM Candidate Experience portal (with presets, Citizenship/Work Permit check, and pre-filled defaults).
   - **1 Vacancy = 1 Hiring Process:** Every candidate triggers `application-received.bpmn`, while the first candidate for a Vacancy ID (e.g. `VAC-FP-2026-01`) starts one shared 24-step `hiring-full-professor.bpmn` workflow across 3 pools and 7 roles.
   - Apache Flink's `RollingHour` CEP window triggers `scale-up-application-receival.bpmn` when `> 5` applications arrive within 1 hour, and receives a completion callback back from Camunda (`PAIS -> CEP`).
2. **Step 2 - Camunda 8 Orchestration & XES Export (`HiringTraceWorker`, `VacancyTraceCollector`):**
   - Collects live task milestones from Camunda 8.8 Zeebe per `vacancyId` and writes an IEEE `.xes` trace file (`XesLogBuilder`).
3. **Step 3 - Live DTU Process Doctor Conformance Audit (`ConformanceChecker`):**
   - Submits the generated `.xes` log and `Full_Professor_Hiring_Compliance.dcr.xml` (25 events, 51 relations) to `https://processdoctor.compute.dtu.dk/` and displays live compliance metrics in the Operations Console.

---

## Quick Start (Full Stack with Docker)

**Prerequisites:** Docker + Docker Compose (`v2.23+`), and at least ~4 GB free RAM.

```bash
git clone -b teja https://github.com/teja-mr-1/DTU-02268-Talent-Management.git
cd DTU-02268-Talent-Management
docker compose up -d --build
```

Wait ~60 seconds for `elasticsearch` and `orchestration` (Camunda 8.8) to become healthy (`docker compose ps`).

### Service URLs
- **Applicant Portal (Oracle HCM UI):** `http://localhost:8090/`
- **Operations & Compliance Console:** `http://localhost:8090/admin.html`
- **Apache Flink UI:** `http://localhost:8081/`
- **Camunda Operate:** `http://localhost:8088/operate` (login: `demo` / `demo`)
- **Camunda Tasklist:** `http://localhost:8088/tasklist` (login: `demo` / `demo`)

---

## How to Test

1. Open the **Operations Console** (`http://localhost:8090/admin.html`).
2. Turn **Auto-Approve Camunda Tasks** and **Auto-Audit Process Doctor** **ON** (or leave them OFF to approve tasks step-by-step in Camunda Tasklist).
3. Open the **Applicant Portal** (`http://localhost:8090/`) and click **Submit** (or **Simulate Traffic Surge (6 Apps)** to trigger Flink CEP).
4. Return to `/admin.html` to watch the 24 hiring milestones, Candidate Roster, Flink Surge Alerts, and Process Doctor verdict (`CONFORMANT`).
5. Click **Reset All Data (Fresh Start)** before starting a new clean run.

---

## Run Without Docker (Simulator Mode) & Tests

If Docker is not running, you can run the Spring Boot + embedded Flink app in local simulator mode:

```bash
cd source/flink-app
./gradlew run
```

Run the unit test suite:

```bash
cd source/flink-app
./gradlew test --no-daemon
```

---

## Repository Structure

```text
DTU-02268-Talent-Management/
├── compose.yaml                              # Full stack: Elasticsearch + Camunda 8.8 + Flink App
├── docker/Dockerfile.applications            # Multi-stage Java 21 + Playwright build
├── models/
│   ├── bpmn/hiring-full-professor.bpmn       # 3-pool executable Full Professor BPMN
│   └── dcr/Full_Professor_Hiring_Compliance.dcr.xml
├── source/
│   ├── application-form/                     # Oracle HCM Applicant Portal & Operations Console UI
│   ├── camunda/                              # Deployed BPMNs (hiring-full-professor, application-received, scale-up)
│   └── flink-app/                            # Spring Boot + Apache Flink CEP + Camunda workers + Process Doctor
└── README.md
```
