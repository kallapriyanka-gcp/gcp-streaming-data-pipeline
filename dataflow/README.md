
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



## 3. Pub/Sub to BigQuery Streaming Pipeline

**File:** `pubsub_to_bigquery.py`

### Overview

Implemented a streaming data pipeline using Google Cloud Pub/Sub, Apache Beam, and BigQuery.

The pipeline reads sales events from Pub/Sub, converts JSON messages into structured records, and writes the records into a BigQuery table.

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
Parse JSON message
    ↓
BigQuery
streaming_sales_dataset.sales_streaming
```

### Technologies Used

- Google Cloud Pub/Sub
- Apache Beam (Python)
- BigQuery
- Python
- Google Cloud CLI

### Implementation

1. Created a Pub/Sub topic and subscription.
2. Published sample sales events in JSON format.
3. Used Apache Beam to read streaming messages from Pub/Sub.
4. Converted JSON messages into structured Python dictionaries.
5. Configured Apache Beam to write records into BigQuery.
6. Created a BigQuery dataset and table to store sales records.
7. Verified the inserted sales record using a BigQuery SQL query.

### BigQuery Table

Project: employee-data-pipeline-501012

Dataset: streaming_sales_dataset

Table: sales_streaming

Location: asia-south1 (Mumbai)

### Table Schema

| Column | Data Type |
|--------|-----------|
| order_id | INTEGER |
| customer | STRING |
| amount | FLOAT |

### Sample Sales Message

```json
{
  "order_id": 1008,
  "customer": "Priyanka",
  "amount": 9500
}
```

### Execution

Run the streaming pipeline locally:

```bash
python dataflow/pubsub_to_bigquery.py
```

Publish a test message:

```bash
gcloud pubsub topics publish sales-data-topic \
  --message='{"order_id":1008,"customer":"Priyanka","amount":9500}'
```

### Verification

Executed the following SQL query in BigQuery:

```sql
SELECT *
FROM `employee-data-pipeline-501012.streaming_sales_dataset.sales_streaming`
ORDER BY order_id DESC;
```

Successfully verified that order 1008 was stored in the BigQuery table.

### Result

Successfully implemented and tested a Pub/Sub to BigQuery streaming pipeline using Apache Beam DirectRunner.

The pipeline was executed locally. Deployment to Google Cloud Dataflow is a future enhancement.