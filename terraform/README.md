\# Terraform Infrastructure



This directory contains the Terraform configuration for the GCP Data Lakehouse and Real-Time Analytics Platform.



\## Managed Infrastructure



Terraform manages the following existing GCP resources:



\### Google Cloud Storage

\- `gcp-lakehouse-realtime-data-2026`

\- `gcp-lakehouse-dataflow-staging-2026`



\### BigQuery

\- `ecommerce\_bronze`

\- `ecommerce\_silver`

\- `ecommerce\_gold`

\- `ecommerce\_streaming`



\### Pub/Sub

\- `ecommerce-orders`

\- `ecommerce-orders-sub`



\## Resources Not Managed by Terraform



The following are intentionally managed by the data pipeline rather than Terraform:



\- BigQuery streaming tables

\- BigQuery analytical views

\- Historical Dataflow jobs

\- Historical Dataproc Serverless batches

\- Google-managed service agents

\- Existing IAM permissions



\## Terraform Workflow



```text

Terraform Configuration

&#x20;       ↓

terraform init

&#x20;       ↓

terraform validate

&#x20;       ↓

terraform plan

&#x20;       ↓

Review changes

&#x20;       ↓

terraform apply

