resource "google_pubsub_topic" "ecommerce_orders" {
  name    = "ecommerce-orders"
  project = var.project_id
}

resource "google_pubsub_subscription" "ecommerce_orders_sub" {
  name    = "ecommerce-orders-sub"
  project = var.project_id
  topic   = google_pubsub_topic.ecommerce_orders.id

  ack_deadline_seconds       = 10
  message_retention_duration = "604800s"

  expiration_policy {
    ttl = "2678400s"
  }
}