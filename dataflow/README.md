
# Google Cloud Dataflow & Apache Beam

## Overview

Google Cloud Dataflow is a managed service used to run batch and streaming data processing pipelines.

Apache Beam is a programming framework used to build data processing pipelines.

Apache Beam pipelines can be executed locally using DirectRunner or on Google Cloud using DataflowRunner.

## 1. Apache Beam Hands-on

**File:** `beam_basics.py`

Implemented basic Apache Beam transformations using the `sales.csv` file.

### Pipeline Flow

```text
sales.csv
    ↓
ReadFromText
    ↓
Parse CSV rows
    ↓
Map transformation
    ↓
Filter transformation
    ↓
ParDo transformation
    ↓
Output
```

### What I implemented

- Read sales data from a CSV file.
- Converted CSV rows into structured Python dictionaries.
- Filtered high-value sales orders.
- Practiced Map and ParDo transformations.
- Executed the pipeline locally using Apache Beam DirectRunner.

## 2. Pub/Sub to Apache Beam Streaming Pipeline

**File:** `pubsub_to_beam.py`

Implemented a basic streaming data pipeline using Google Cloud Pub/Sub and Apache Beam.

### Pipeline Architecture

```text
Sales event
    ↓
Pub/Sub Topic
sales-data-topic
    ↓
Pub/Sub Subscription
sales-data-topic-sub
    ↓
Apache Beam (DirectRunner)
    ↓
Decode message
    ↓
Print received message
```

### Implementation

- Created a Pub/Sub topic and subscription.
- Published sample sales messages in JSON format.
- Configured Apache Beam in streaming mode.
- Used ReadFromPubSub to consume messages from the subscription.
- Decoded incoming messages into UTF-8 strings.
- Executed the pipeline locally using DirectRunner.
- Successfully received and displayed a sales event.

### Sample Message

```json
{
  "order_id": 1007,
  "customer": "Priyanka",
  "amount": 8500
}
```

### Execution

```bash
python dataflow/pubsub_to_beam.py
```

The pipeline uses `streaming=True` in its PipelineOptions configuration.

### Result

Successfully consumed a Pub/Sub message using Apache Beam and displayed the received JSON message in the terminal.

**Note:** This pipeline was executed locally using Apache Beam DirectRunner. Deployment to Google Cloud Dataflow and integration with BigQuery are planned as the next steps.