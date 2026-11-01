"""PyFlink job: route applications to DCR and alert on too many open ones."""
import json
import logging
import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import FlatMapFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

import dcr_client
import routing

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
THRESHOLD = int(os.getenv("THRESHOLD", "10"))  # max open applications per position
KAFKA_JAR = os.getenv(
    "KAFKA_JAR",
    "file://" + os.path.abspath("jars/flink-sql-connector-kafka-3.3.0-1.20.jar"),
)



# ---------- Kafka helpers ----------
def kafka_source(topic: str, group: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(group)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


# ---------- 1. Routing ----------
class StartCase(FlatMapFunction):
    """Pick the DCR graph by position, create a case, emit a `case-started` event."""

    def open(self, runtime_context):
        self.dcr = dcr_client.get_client()

    def flat_map(self, value):
        try:
            app = json.loads(value)
            position = app["position"]
            graph_id = routing.graph_id_for(position)
            case_id = self.dcr.create_case(graph_id)
        except Exception:
            logging.exception("Failed to route application: %s", value)
            return  # TODO: send to a dead-letter topic
        yield json.dumps({
            "applicationId": app["id"],
            "position": position,
            "graphId": graph_id,
            "caseId": case_id,
        })


# ---------- 2. Open-application tracking ----------
class OpenApplicationCounter(KeyedProcessFunction):
    """Keyed by position. Input: (position, +1/-1). Emits an alert JSON string
    when the open count crosses THRESHOLD (once per crossing, not per event)."""

    def open(self, runtime_context):
        self.count = runtime_context.get_state(ValueStateDescriptor("open_count", Types.INT()))
        self.alerting = runtime_context.get_state(ValueStateDescriptor("alerting", Types.BOOLEAN()))

    def process_element(self, value, ctx):
        position, delta = value
        count = max(0, (self.count.value() or 0) + delta)
        self.count.update(count)
        logging.warning("open[%s] = %d", position, count)

        alerting = bool(self.alerting.value())
        if count > THRESHOLD and not alerting:
            self.alerting.update(True)
            yield json.dumps({
                "type": "TOO_MANY_OPEN_APPLICATIONS",
                "position": position,
                "open": count,
                "threshold": THRESHOLD,
            })
        elif count <= THRESHOLD and alerting:
            self.alerting.update(False)  # re-arm


def main():
    logging.basicConfig(level=logging.WARNING)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # readable output for a demo
    # Locally we load the jar from ./jars; inside the Flink image it's already in /opt/flink/lib.
    if KAFKA_JAR and os.path.exists(KAFKA_JAR.replace("file://", "")):
        env.add_jars(KAFKA_JAR)

    apps = env.from_source(
        kafka_source("applications", "flink-applications"),
        WatermarkStrategy.no_watermarks(), "applications",
    )
    closed = env.from_source(
        kafka_source("application-closed", "flink-closed"),
        WatermarkStrategy.no_watermarks(), "application-closed",
    )

    # Routing -> DCR
    apps.flat_map(StartCase(), output_type=Types.STRING()) \
        .sink_to(kafka_sink("case-started"))

    # Tracking
    pair = Types.TUPLE([Types.STRING(), Types.INT()])

    def parse(delta):
        def _f(raw):
            return (json.loads(raw).get("position", "unknown"), delta)
        return _f

    plus = apps.map(parse(+1), output_type=pair)
    minus = closed.map(parse(-1), output_type=pair)

    alerts = (
        plus.union(minus)
        .key_by(lambda t: t[0], key_type=Types.STRING())
        .process(OpenApplicationCounter(), output_type=Types.STRING())
    )
    alerts.sink_to(kafka_sink("alerts"))
    alerts.print()

    env.execute("application-router")


if __name__ == "__main__":
    main()
