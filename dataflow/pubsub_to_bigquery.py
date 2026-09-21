
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "employee-data-pipeline-501012"

SUBSCRIPTION = (
    f"projects/{PROJECT_ID}/"
    "subscriptions/sales-data-topic-sub"
)

TABLE_ID = (
    f"{PROJECT_ID}:"
    "streaming_sales_dataset.sales_streaming"
)


def parse_message(message):
    """Convert a Pub/Sub JSON message into a BigQuery row."""

    data = json.loads(message.decode("utf-8"))

    return {
        "order_id": int(data["order_id"]),
        "customer": str(data["customer"]),
        "amount": float(data["amount"]),
    }


options = PipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    runner="DirectRunner",
)


with beam.Pipeline(options=options) as pipeline:

    (
        pipeline
        | "Read Pub/Sub messages"
        >> beam.io.ReadFromPubSub(
            subscription=SUBSCRIPTION
        )
        | "Parse JSON messages"
        >> beam.Map(parse_message)
        | "Write sales to BigQuery"
        >> beam.io.WriteToBigQuery(
            table=TABLE_ID,
            schema=(
                "order_id:INTEGER,"
                "customer:STRING,"
                "amount:FLOAT"
            ),
            create_disposition=(
                beam.io.BigQueryDisposition.CREATE_NEVER
            ),
            write_disposition=(
                beam.io.BigQueryDisposition.WRITE_APPEND
            ),
            method=(
                beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
            ),
        )
    )