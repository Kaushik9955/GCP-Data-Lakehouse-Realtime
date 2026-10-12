resource "google_storage_bucket" "lakehouse_data" {
  name                        = "gcp-lakehouse-realtime-data-2026"
  location                    = "ASIA-SOUTH1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  encryption {
    customer_managed_encryption_enforcement_config {
      restriction_mode = "NotRestricted"
    }

    customer_supplied_encryption_enforcement_config {
      restriction_mode = "FullyRestricted"
    }

    google_managed_encryption_enforcement_config {
      restriction_mode = "NotRestricted"
    }
  }
}

resource "google_storage_bucket" "dataflow_staging" {
  name                        = "gcp-lakehouse-dataflow-staging-2026"
  location                    = "ASIA-SOUTH1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 604800
  }
}