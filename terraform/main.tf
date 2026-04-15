terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

provider "google" {
  alias   = "beta"
  project = var.project
  region  = var.region
}

# Service account that the Cloud Run Jobs will run as
resource "google_service_account" "jobs_sa" {
  account_id   = "pure-jobs-sa"
  display_name = "Pure Data Pipeline Jobs Service Account"
}

# Service account used by Workflows to invoke the jobs
resource "google_service_account" "workflows_sa" {
  account_id   = "pure-workflows-sa"
  display_name = "Pure Orchestration Workflows Service Account"
}

# Grant the workflows SA the rights to run and view Cloud Run Jobs
resource "google_project_iam_member" "workflows_run_invoker" {
  project = var.project
  role    = "roles/run.jobsInvoker"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

resource "google_project_iam_member" "workflows_run_viewer" {
  project = var.project
  role    = "roles/run.viewer"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

# Grant the jobs SA the permissions it needs (BigQuery, Storage, Secret Manager)
resource "google_project_iam_member" "jobs_bq_editor" {
  project = var.project
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

resource "google_project_iam_member" "jobs_bq_jobuser" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

resource "google_project_iam_member" "jobs_storage_admin" {
  project = var.project
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

resource "google_project_iam_member" "jobs_secret_accessor" {
  project = var.project
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

# Create Cloud Run Jobs (v2). We use the google-beta provider for v2 resources.
# In each job we set the JOB_TYPE environment variable so the container runs a single job.

resource "google_cloud_run_v2_job" "ingest_persons" {
  provider = google.beta
  name     = "ingest-persons"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        env {
          name  = "JOB_TYPE"
          value = "INGEST_PERSONS"
        }
      }

      # number of tasks (parallelism) and retry behaviour
      task_count  = 1
      max_retries = 0
    }

    # The service account the job will run as
    service_account = google_service_account.jobs_sa.email
  }
}

resource "google_cloud_run_v2_job" "ingest_fingerprints" {
  provider = google.beta
  name     = "ingest-fingerprint-concepts"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        env {
          name  = "JOB_TYPE"
          value = "INGEST_FINGERPRINT_CONCEPTS"
        }
        resources {
          limits = {
            memory = "4Gi"
            cpu    = "2"
          }
        }
      }

      task_count  = 1
      max_retries = 0
    }

    service_account = google_service_account.jobs_sa.email
  }
}

resource "google_cloud_run_v2_job" "ingest_publications" {
  provider = google.beta
  name     = "ingest-publications"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        env {
          name  = "JOB_TYPE"
          value = "INGEST_PUBLICATIONS"
        }
        resources {
          limits = {
            memory = "4Gi"
            cpu    = "2"
          }
        }
      }

      task_count  = 1
      max_retries = 0
    }

    service_account = google_service_account.jobs_sa.email
  }
}

resource "google_cloud_run_v2_job" "ingest_researchers" {
  provider = google.beta
  name     = "ingest-researchers"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        env {
          name  = "JOB_TYPE"
          value = "INGEST_RESEARCHERS"
        }
        resources {
          limits = {
            memory = "8Gi"
            cpu    = "4"
          }
        }
      }

      task_count  = 1
      max_retries = 0
    }

    service_account = google_service_account.jobs_sa.email
  }
}

# Create the Workflow that orchestrates these jobs. The source contents reference
# a workflow definition that triggers the three ingest jobs in parallel then runs the researchers job.
resource "google_workflows_workflow" "orchestrate_jobs" {
  name     = "orchestrate-jobs"
  region   = var.region
  description = "Orchestrate ingestion jobs and kick off researchers embedding on success"

  source_contents = file("./workflows/orchestrate_jobs.yaml")

  service_account = google_service_account.workflows_sa.email
}
