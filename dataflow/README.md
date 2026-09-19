# Google Cloud Dataflow

## What is Dataflow?

Google Cloud Dataflow is a managed service used to run data processing pipelines for batch and streaming data.

## Apache Beam and Dataflow

Apache Beam is a framework used to build data processing pipelines.

Google Cloud Dataflow is a managed service that can run Apache Beam pipelines.

```text
Apache Beam
    ↓
Data processing pipeline
    ↓
Google Cloud Dataflow


# Dataflow & Apache Beam

## Overview

Google Cloud Dataflow is a managed service used to run data processing pipelines.

Apache Beam is the programming framework used to build the pipeline.

## Apache Beam Hands-on

The `beam_basics.py` file demonstrates basic Apache Beam transformations using the `sales.csv` file.

### Pipeline flow

```text
sales.csv
    ↓
ReadFromText
    ↓
Parse CSV rows
    ↓
Map transformation
    ↓
ParDo transformation
    ↓
Output