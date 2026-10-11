variable "project_id" { type = string }
variable "project_number" { type = string }
variable "region" { type = string }
variable "zone" { type = string }
variable "name_prefix" { type = string }
variable "compute_enabled" { type = bool }
variable "worker_count" { type = number }
variable "coordinator_machine_type" { type = string }
variable "worker_machine_type" { type = string }
variable "debian12_image" { type = string }
variable "subnet_cidr" { type = string }
variable "coordinator_boot_disk_size_gb" { type = number }
variable "coordinator_data_disk_size_gb" { type = number }
variable "worker_boot_disk_size_gb" { type = number }
variable "worker_data_disk_size_gb" { type = number }
variable "service_revision" { type = string }
variable "release_object" {
  type = string

  validation {
    condition     = var.release_object == "releases/${var.service_revision}.tar.gz"
    error_message = "release_object must name the tar archive for service_revision."
  }
}
variable "release_sha256" { type = string }
variable "worker_ssh_public_key" { type = string }
variable "secret_ids" {
  type = object({
    model_auth = string
    git_read   = string
    worker_ssh = string
  })
}
variable "secret_versions" {
  type = object({
    model_auth = string
    git_read   = string
    worker_ssh = string
  })
}
variable "optional_integration_secrets" {
  type = map(object({
    secret_id = string
    version   = string
  }))
  default = {}

  validation {
    condition = alltrue([
      for name, integration in var.optional_integration_secrets :
      !contains(["model_auth", "git_read", "worker_ssh"], name) &&
      can(regex("^[A-Za-z0-9_-]{1,255}$", integration.secret_id)) &&
      can(regex("^[1-9][0-9]*$", integration.version))
    ])
    error_message = "Optional integration secrets must use non-reserved map keys, valid Secret Manager IDs and positive pinned version numbers."
  }

  validation {
    condition = length(distinct(concat(
      values(var.secret_ids),
      [for integration in values(var.optional_integration_secrets) : integration.secret_id],
    ))) == length(var.secret_ids) + length(var.optional_integration_secrets)
    error_message = "Each base and optional integration secret container must have a distinct Secret Manager ID."
  }
}

variable "coordinator_workflow" {
  type    = string
  default = "pilot"
  validation {
    condition     = contains(["pilot", "linear"], var.coordinator_workflow)
    error_message = "coordinator_workflow must be pilot or linear."
  }
}
variable "coordinator_secret_env" {
  type    = map(string)
  default = {}
  validation {
    condition = alltrue([
      for env, key in var.coordinator_secret_env :
      contains([
        "LINEAR_API_KEY",
        "LINEAR_API_TOKEN",
        "FACTORY_HIGH_CLIENT_SECRET",
        "FACTORY_HIGH_WEBHOOK_SECRET",
        "FACTORY_DAYBREAK_CLIENT_SECRET",
        "FACTORY_DAYBREAK_WEBHOOK_SECRET",
      ], env) && contains(keys(var.optional_integration_secrets), key)
      ]) && length(distinct(values(var.coordinator_secret_env))) == length(var.coordinator_secret_env) && (
      var.coordinator_workflow == "linear" ? (
        alltrue([for env in ["LINEAR_API_KEY", "LINEAR_API_TOKEN"] : contains(keys(var.coordinator_secret_env), env)]) &&
        (contains(keys(var.coordinator_secret_env), "FACTORY_HIGH_CLIENT_SECRET") == contains(keys(var.coordinator_secret_env), "FACTORY_HIGH_WEBHOOK_SECRET")) &&
        (contains(keys(var.coordinator_secret_env), "FACTORY_DAYBREAK_CLIENT_SECRET") == contains(keys(var.coordinator_secret_env), "FACTORY_DAYBREAK_WEBHOOK_SECRET"))
      ) : length(var.coordinator_secret_env) == 0
    )
    error_message = "Linear requires the default API key and webhook secret plus zero or both references for each named app; pilot must have no credential mapping."
  }
}
variable "enable_linear_webhook" {
  type    = bool
  default = false
  validation {
    condition     = !var.enable_linear_webhook || (var.enable_https_iap && var.coordinator_workflow == "linear")
    error_message = "The public webhook requires HTTPS ingress and linear coordinator workflow."
  }
}
variable "archive_bucket_name" {
  type     = string
  nullable = true
}
variable "backup_bucket_name" {
  type     = string
  nullable = true
}
variable "release_bucket_name" {
  type     = string
  nullable = true
}
variable "backup_retention_days" {
  type     = number
  nullable = true
}
variable "snapshot_retention_days" { type = number }
variable "billing_account_id" { type = string }
variable "budget_amount_usd" { type = number }
variable "notification_emails" { type = set(string) }
variable "iap_operator_emails" { type = set(string) }
variable "enable_https_iap" { type = bool }
variable "viewer_hostname" {
  type     = string
  nullable = true
}
variable "iap_viewer_emails" { type = set(string) }
variable "iap_viewer_domains" {
  description = "Google Workspace or Cloud Identity managed domains allowed through dashboard IAP. Verify domain and project organization eligibility before deployment."
  type        = set(string)
  default     = []

  validation {
    condition     = alltrue([for domain in var.iap_viewer_domains : can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$", domain))])
    error_message = "iap_viewer_domains must contain bare lowercase DNS domains, without wildcards, email addresses or principal prefixes."
  }
}

variable "iap_google_managed_oauth_confirmed" { type = bool }
