variable "project" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "GCP region to deploy resources"
  type        = string
  default     = "us-central1"
}

variable "image" {
  description = "Container image (fully qualified) to use for Cloud Run Jobs"
  type        = string
}
