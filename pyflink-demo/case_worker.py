"""Progresses DCR cases and reports when they finish.

A DCR case is passive: something has to execute its events. This worker consumes
`case-started`, then repeatedly reads the case state and executes the next enabled,
not-yet-executed event. When nothing is left to execute and the case is accepting,
it publishes `application-closed` so Flink can decrement its open-application count.
If nothing is executable but the case is NOT accepting, it is stuck (pending events that
can't be enabled): we log it and keep it open, re-checking every 30s.

DCR_MODE=mock steps through a fake 2-event case; DCR_MODE=real drives the real API.
Use a large --min-seconds to let applications pile up and trigger the alert.
"""
import argparse
import heapq
import json
import random
import time

from confluent_kafka import Consumer, Producer

import dcr_client

STEPS_ESTIMATE = 2  # events in the demo graph; only used to spread the delay


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--min-seconds", type=float, default=2)
    p.add_argument("--max-seconds", type=float, default=15)
    p.add_argument("--bootstrap", default="localhost:9092")
    args = p.parse_args()

    dcr = dcr_client.get_client()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": "case-worker",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["case-started"])
    producer = Producer({"bootstrap.servers": args.bootstrap})

    pending, counter = [], 0  # heap of (due_time, counter, event)

    def schedule(event, delay):
        nonlocal counter
        heapq.heappush(pending, (time.time() + delay, counter, event))
        counter += 1

    def step_delay():  # total duration (min..max) spread over the graph's steps
        return random.uniform(args.min_seconds, args.max_seconds) / STEPS_ESTIMATE

    print("case worker running...")
    while True:
        msg = consumer.poll(0.2)
        if msg is not None and not msg.error():
            schedule(json.loads(msg.value()), step_delay())

        while pending and pending[0][0] <= time.time():
            _, _, event = heapq.heappop(pending)
            graph_id, case_id = event["graphId"], event["caseId"]
            try:
                state = dcr.get_case_state(graph_id, case_id)
                todo = state.executable()
                if todo:
                    dcr.execute_event(graph_id, case_id, todo[0].id)
                    print(f"case {case_id[:8]} executed {todo[0].id} ({todo[0].label})")
                    schedule(event, step_delay())
                    continue
                if not state.accepting:
                    print(f"case {case_id[:8]} is stuck: nothing executable and not accepting")
                    schedule(event, 30)
                    continue
            except Exception as e:
                print(f"case {case_id[:8]} error, retrying in 5s: {e}")
                schedule(event, 5)
                continue

            closed = {"id": event["applicationId"], "position": event["position"]}
            producer.produce("application-closed", value=json.dumps(closed))
            producer.poll(0)
            print(f"closed {closed['position']} {closed['id'][:8]}")


if __name__ == "__main__":
    main()
