import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "employee-data-pipeline-501012"

SUBSCRIPTION = (
    "projects/employee-data-pipeline-501012/"
    "subscriptions/sales-data-topic-sub"
)


options = PipelineOptions(
    streaming=True
)


with beam.Pipeline(options=options) as pipeline:

    messages = (
        pipeline
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
            subscription=SUBSCRIPTION
        )
        | "Decode message" >> beam.Map(
            lambda message: message.decode("utf-8")
        )
        | "Print message" >> beam.Map(print)
    )