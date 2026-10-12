output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP region"
  value       = var.region
}

output "lakehouse_data_bucket" {
  description = "Main lakehouse GCS bucket"
  value       = google_storage_bucket.lakehouse_data.name
}

output "dataflow_staging_bucket" {
  description = "Dataflow staging GCS bucket"
  value       = google_storage_bucket.dataflow_staging.name
}

output "ecommerce_streaming_dataset" {
  description = "BigQuery streaming dataset"
  value       = google_bigquery_dataset.ecommerce_streaming.dataset_id
}

output "ecommerce_orders_topic" {
  description = "Pub/Sub orders topic"
  value       = google_pubsub_topic.ecommerce_orders.name
}

output "ecommerce_orders_subscription" {
  description = "Pub/Sub orders subscription"
  value       = google_pubsub_subscription.ecommerce_orders_sub.name
}