output "jobs_service_account_email" {
  value = google_service_account.jobs_sa.email
}

output "workflows_service_account_email" {
  value = google_service_account.workflows_sa.email
}

output "orchestration_workflow_name" {
  value = google_workflows_workflow.orchestrate_jobs.name
}
