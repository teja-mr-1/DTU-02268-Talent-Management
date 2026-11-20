# Local Full Professor Hiring Pipeline (Steps 1, 2 & 3)

Standalone copy (not a git repo) running the full **Step 1 -> Step 2 -> Step 3** pipeline:

1. **Step 1 (Intake & CEP):** Applicant Portal (`POST /api/applications`) + HR/Department Workflow (`POST /api/vacancies/...`) -> Apache Flink `RollingHour` CEP (>5 apps/hr triggers scale-up alert) -> Camunda 8 (`hiring-full-professor.bpmn`) or built-in local simulator when Docker is not running.
2. **Step 2 (Per-Vacancy XES Builder):** Camunda `emit-hiring-trace` worker (`HiringTraceWorker`) or Event Source 2 -> Flink `VacancyTraceCollector` keyed by `vacancyId` -> publishes `data/xes/<vacancyId>.xes` via `XesLogBuilder`.
3. **Step 3 (Live DTU Process Doctor Audit):** Flink `ConformanceCheckSink` -> `ConformanceChecker` (Playwright) -> uploads `.xes` + bundled `Full_Professor_Hiring_Compliance.dcr.xml` to `https://processdoctor.compute.dtu.dk/` -> exposes results at `GET /api/conformance` and `GET /api/vacancies`.

## Run Locally (No Docker Needed)

```bash
cd /mnt/linuxdata/fraud-brain/proess/local-full-pipeline-demo/source/flink-app
./gradlew run
```

Then open: `http://localhost:8090`

## Run Tests

```bash
cd /mnt/linuxdata/fraud-brain/proess/local-full-pipeline-demo/source/flink-app
./gradlew test --no-daemon
```
