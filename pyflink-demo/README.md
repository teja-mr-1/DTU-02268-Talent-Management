# Flink + DCR demo

```
generator ─► applications ─► Flink job ─► (creates DCR case) ─► case-started ─► case-worker
                                 ▲  │                                              │
                application-closed ◄───────────────────────────────────────────────┘
                                    └─► alerts ─► alert-watcher (logs)
```

Requires only Docker (Compose v2). DCR is mocked by default, so no account is needed.

## Run
```
docker compose up --build
```
First build takes a few minutes (PyFlink install). After that:

| What | How |
|---|---|
| Flink web UI (job graph, metrics) | http://localhost:8081 |
| Alerts | `docker compose logs -f alert-watcher` |
| Routing + counts | `docker compose logs -f taskmanager` (look for `open[engineer] = 7`) |
| Case progress | `docker compose logs -f case-worker` |
| Stop / reset | `docker compose down` |

## Knobs
Optional: `cp .env.example .env` and edit (`.env` is git-ignored; defaults work without it).
`THRESHOLD`, `GEN_RATE`, `WORKER_MIN_SECONDS`, `WORKER_MAX_SECONDS`.

**Force an alert for the demo:** make cases slow so applications pile up:
```
WORKER_MIN_SECONDS=60 WORKER_MAX_SECONDS=90 docker compose up -d case-worker
```
You get one alert per position when it passes THRESHOLD, and it re-arms once the
count drops back down. Restore with `docker compose up -d case-worker`.

## Using the real DCR API
In `.env`:
```
DCR_MODE=real
DCR_USERNAME=...
DCR_PASSWORD=...
DCR_GRAPH_ID=...                        # graph used for every position
```
A DCR case is passive: Flink creates it, and `case-worker` executes each enabled,
not-yet-executed event in turn. When none are left and the case is accepting, it reports
the application closed. A case that is not accepting and has nothing to execute is logged
as stuck and stays open.

The create-sim call returns no case id, so `dcr_client.py` finds the new case by diffing
the sim list before and after. That's safe while the Flink routing step runs at
parallelism 1 (as it does here), but sims created by someone else at the same moment
make it ambiguous (it then takes the highest id and logs a warning).

## Running outside Docker
See `requirements.txt` (Python 3.9-3.11 + JDK needed), download the Kafka connector
jar into `./jars/`, point `KAFKA_BOOTSTRAP` at a Kafka reachable from your host
(`localhost:9092` works with this compose file's `kafka` service), then `python job.py`.
