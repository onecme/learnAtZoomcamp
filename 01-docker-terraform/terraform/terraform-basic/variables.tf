variable "project" {
  description = "GCP Project ID"
  default     = "****"
}

variable "region" {
  description = "GCP Region"
  default     = "us-central1"
}

variable "location" {
  description = "GCP Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name (must be globally unique)"
  default     = "terraform-demo-terra-bucket-elyas-2026"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
