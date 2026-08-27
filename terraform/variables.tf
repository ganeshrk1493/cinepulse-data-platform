variable "project_id" {
  type        = string
  description = "The GCP Project ID"
}

variable "region" {
  type        = string
  description = "Default GCP Region for compute resources"
  default     = "us-central1"
}

variable "storage_location" {
  type        = string
  description = "Location for BigQuery datasets and GCS buckets"
  default     = "US"
}