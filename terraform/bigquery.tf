resource "google_bigquery_dataset" "ecommerce_bronze" {
  project    = var.project_id
  dataset_id = "ecommerce_bronze"
  location   = "asia-south1"
}

resource "google_bigquery_dataset" "ecommerce_silver" {
  project    = var.project_id
  dataset_id = "ecommerce_silver"
  location   = "asia-south1"
}
resource "google_bigquery_dataset" "ecommerce_gold" {
  project    = var.project_id
  dataset_id = "ecommerce_gold"
  location   = "asia-south1"
}

resource "google_bigquery_dataset" "ecommerce_streaming" {
  project    = var.project_id
  dataset_id = "ecommerce_streaming"
  location   = "asia-south1"
}