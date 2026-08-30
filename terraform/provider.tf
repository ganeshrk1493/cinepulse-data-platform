terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # REMOTE STATE BACKEND
  backend "gcs" {
    bucket = "cinepulse-tfstate-cinepulse-analytics"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}