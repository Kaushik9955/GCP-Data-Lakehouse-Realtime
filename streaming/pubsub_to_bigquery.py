import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# ---------------------------------------------------------
# GCP Configuration
# ---------------------------------------------------------

PROJECT_ID = "gcp-data-lakehouse-realtime"

SUBSCRIPTION = (
    "projects/gcp-data-lakehouse-realtime/"
    "subscriptions/ecommerce-orders-sub"
)

BIGQUERY_TABLE = (
    "gcp-data-lakehouse-realtime."
    "ecommerce_streaming.orders_stream"
)


# ---------------------------------------------------------
# Parse Pub/Sub Message
# ---------------------------------------------------------

def parse_message(message):
    data = json.loads(message.decode("utf-8"))

    return {
        "order_id": data.get("order_id"),
        "customer_id": data.get("customer_id"),
        "product_id": data.get("product_id"),
        "quantity": int(data.get("quantity", 0)),
        "amount": float(data.get("amount", 0)),
        "status": data.get("status"),
        "event_time": data.get("event_time"),
        "processed_at": None,
    }


# ---------------------------------------------------------
# BigQuery Schema
# ---------------------------------------------------------

BQ_SCHEMA = {
    "fields": [
        {
            "name": "order_id",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "customer_id",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "product_id",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "quantity",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "amount",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },
        {
            "name": "status",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "event_time",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },
        {
            "name": "processed_at",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },
    ]
}


# ---------------------------------------------------------
# Pipeline Options
# ---------------------------------------------------------

# Do NOT hard-code temp_location or staging_location here.
# They will be supplied when submitting the Dataflow job.

options = PipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    save_main_session=True,
)


# ---------------------------------------------------------
# Apache Beam Pipeline
# ---------------------------------------------------------

with beam.Pipeline(options=options) as pipeline:

    (
        pipeline
        | "ReadFromPubSub"
        >> beam.io.ReadFromPubSub(
            subscription=SUBSCRIPTION
        )

        | "ParseJSON"
        >> beam.Map(parse_message)

        | "WriteToBigQuery"
        >> beam.io.WriteToBigQuery(
            table=BIGQUERY_TABLE,
            schema=BQ_SCHEMA,
            project=PROJECT_ID,
            method="STREAMING_INSERTS",
            create_disposition=(
                beam.io.BigQueryDisposition.CREATE_NEVER
            ),
            write_disposition=(
                beam.io.BigQueryDisposition.WRITE_APPEND
            ),
        )
    )