"""Mock job application source: writes JSON to the `applications` topic."""
import argparse
import json
import random
import time
import uuid

from confluent_kafka import Producer

POSITIONS = ["assistant_professor", "associate_professor", "phd", "postdoc"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=0.1, help="applications per second")
    p.add_argument("--count", type=int, default=0, help="stop after N (0 = forever)")
    p.add_argument("--bootstrap", default="localhost:9092")
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})
    sent = 0
    while args.count == 0 or sent < args.count:
        app = {
            "id": str(uuid.uuid4()),
            "position": random.choice(POSITIONS),
            "candidate": f"candidate-{random.randint(1000, 9999)}",
            "received_at": time.time(),
        }
        producer.produce("applications", key=app["id"], value=json.dumps(app))
        producer.poll(0)
        sent += 1
        print(f"sent #{sent}: {app['position']} {app['id'][:8]}")
        time.sleep(1.0 / args.rate)
    producer.flush()


if __name__ == "__main__":
    main()
