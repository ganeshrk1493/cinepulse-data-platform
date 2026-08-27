# ------------------------------------------------------------------------------
# 1. GOOGLE CLOUD STORAGE (DATA LAKE)
# ------------------------------------------------------------------------------
# GCS bucket names must be globally unique across all of Google Cloud.
# We append the project_id to ensure global uniqueness.
resource "google_storage_bucket" "data_lake" {
  name          = "cinepulse-lake-${var.project_id}"
  location      = var.storage_location
  force_destroy = true # Allows easy teardown during development even if files exist

  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }
}

# ------------------------------------------------------------------------------
# 2. CLOUD PUB/SUB (REAL-TIME STREAMING BUFFER)
# ------------------------------------------------------------------------------
resource "google_pubsub_topic" "movie_stream_topic" {
  name = "reddit-movie-stream"
}

# Pull subscription for downstream consumers (or Dataflow streaming engine)
resource "google_pubsub_subscription" "movie_stream_sub" {
  name  = "reddit-movie-stream-sub"
  topic = google_pubsub_topic.movie_stream_topic.name

  # Retain unacknowledged messages for 1 day
  message_retention_duration = "86400s"
  retain_acked_messages      = false
  ack_deadline_seconds       = 20
}

# ------------------------------------------------------------------------------
# 3. BIGQUERY DATASETS (MEDALLION WAREHOUSE LAYERS)
# ------------------------------------------------------------------------------
# Bronze Layer: Raw Ingestion Logs
resource "google_bigquery_dataset" "bronze" {
  dataset_id  = "cinepulse_bronze"
  description = "Raw ingested streaming and batch tables"
  location    = var.storage_location

  delete_contents_on_destroy = true
}

# Silver Layer: Cleansed Dimensions and Facts
resource "google_bigquery_dataset" "silver" {
  dataset_id  = "cinepulse_silver"
  description = "Cleansed, conformed dimensions and facts transformed by dbt"
  location    = var.storage_location

  delete_contents_on_destroy = true
}

# Gold Layer: Aggregated Business Marts
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "cinepulse_gold"
  description = "Analytical data marts consumed by Looker Studio"
  location    = var.storage_location

  delete_contents_on_destroy = true
}