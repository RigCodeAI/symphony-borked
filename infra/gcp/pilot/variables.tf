variable "project_id" {
  description = "GCP project for this pilot."
  type        = string
  default     = "factory-511117"
}

variable "region" {
  description = "Region for the network, NAT, backups and VMs."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the coordinator and its zonal persistent disks."
  type        = string
  default     = "us-central1-a"
}

variable "name_prefix" {
  description = "Short prefix for resource names."
  type        = string
  default     = "rig-factory"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,17}$", var.name_prefix))
    error_message = "name_prefix must be 3-18 lower-case letters, digits or hyphens and start with a letter."
  }
}

variable "compute_enabled" {
  description = "Create the coordinator and worker VMs. Setting false removes VMs while retaining boot/data disks, buckets and service accounts."
  type        = bool
  default     = false
}

variable "worker_count" {
  description = "Number of pilot workers. Expansion beyond one is held until workers have separate service identities and host-key write scopes."
  type        = number
  default     = 1

  validation {
    condition     = var.worker_count == floor(var.worker_count) && var.worker_count >= 0 && var.worker_count <= 1
    error_message = "worker_count must be 0 or 1 for the initial pilot."
  }
}

variable "coordinator_machine_type" {
  description = "Coordinator machine type."
  type        = string
  default     = "e2-standard-2"
}

variable "worker_machine_type" {
  description = "Worker machine type."
  type        = string
  default     = "n2-standard-16"
}

variable "debian12_image" {
  description = "Exact Debian 12 GCE image self-link, for example projects/debian-cloud/global/images/debian-12-bookworm-v20260902. Do not use an image family."
  type        = string

  validation {
    condition     = can(regex("^projects/debian-cloud/global/images/debian-12-bookworm-v[0-9]{8}$", var.debian12_image))
    error_message = "debian12_image must be an exact dated Debian 12 Bookworm image in debian-cloud."
  }
}

variable "subnet_cidr" {
  description = "Private subnet CIDR."
  type        = string
  default     = "10.42.0.0/20"

  validation {
    condition     = can(cidrhost(var.subnet_cidr, 0))
    error_message = "subnet_cidr must be a valid IPv4 CIDR block."
  }
}

variable "coordinator_boot_disk_size_gb" {
  description = "Coordinator persistent boot disk size."
  type        = number
  default     = 30

  validation {
    condition     = var.coordinator_boot_disk_size_gb >= 30 && var.coordinator_boot_disk_size_gb == floor(var.coordinator_boot_disk_size_gb)
    error_message = "coordinator_boot_disk_size_gb must be a whole number of at least 30 GiB."
  }
}

variable "coordinator_data_disk_size_gb" {
  description = "Coordinator retained SQLite/state disk size."
  type        = number
  default     = 100

  validation {
    condition     = var.coordinator_data_disk_size_gb >= 100 && var.coordinator_data_disk_size_gb == floor(var.coordinator_data_disk_size_gb)
    error_message = "coordinator_data_disk_size_gb must be a whole number of at least 100 GiB."
  }
}

variable "worker_boot_disk_size_gb" {
  description = "Worker persistent boot disk size."
  type        = number
  default     = 50

  validation {
    condition     = var.worker_boot_disk_size_gb >= 50 && var.worker_boot_disk_size_gb == floor(var.worker_boot_disk_size_gb)
    error_message = "worker_boot_disk_size_gb must be a whole number of at least 50 GiB."
  }
}

variable "worker_data_disk_size_gb" {
  description = "Worker retained workspace disk size."
  type        = number
  default     = 500

  validation {
    condition     = var.worker_data_disk_size_gb >= 500 && var.worker_data_disk_size_gb == floor(var.worker_data_disk_size_gb)
    error_message = "worker_data_disk_size_gb must be a whole number of at least 500 GiB."
  }
}

variable "service_revision" {
  description = "Pinned Symphony service commit SHA included in the deployed release archive."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.service_revision))
    error_message = "service_revision must be a 40-character lower-case Git SHA."
  }
}

variable "release_object" {
  description = "Object path for the pinned release archive, normally releases/<service_revision>.tar.gz."
  type        = string

  validation {
    condition     = var.release_object == "releases/${var.service_revision}.tar.gz"
    error_message = "release_object must name the tar archive for service_revision."
  }
}

variable "release_sha256" {
  description = "SHA-256 of the pinned deployment tar archive."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{64}$", var.release_sha256))
    error_message = "release_sha256 must be a 64-character lower-case hexadecimal digest."
  }
}

variable "worker_ssh_public_key" {
  description = "Public half of the worker SSH key. The private half is installed separately as the worker-ssh Secret Manager version."
  type        = string

  validation {
    condition     = can(regex("^ssh-ed25519 [A-Za-z0-9+/]+={0,2}([^\\r\\n]*)$", trimspace(var.worker_ssh_public_key)))
    error_message = "worker_ssh_public_key must be a single-line ssh-ed25519 public key. Never pass the private key here."
  }
}

variable "secret_ids" {
  description = "Secret Manager container names. Terraform creates only empty containers and IAM; values are loaded outside Terraform."
  type = object({
    model_auth = string
    git_read   = string
    worker_ssh = string
  })
  default = {
    model_auth = "model-auth"
    git_read   = "git-read"
    worker_ssh = "worker-ssh"
  }
}

variable "secret_versions" {
  description = "Pinned Secret Manager version numbers for the base runtime secrets. Values contain version identifiers only, never secret payloads."
  type = object({
    model_auth = string
    git_read   = string
    worker_ssh = string
  })

  validation {
    condition = alltrue([
      for version in values(var.secret_versions) : can(regex("^[1-9][0-9]*$", version))
    ])
    error_message = "Each secret version must be a positive Secret Manager version number."
  }
}

variable "optional_integration_secrets" {
  description = "Optional empty secret containers and pinned version references for integrations used by the coordinator. Do not put secret values here."
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

variable "archive_bucket_name" {
  description = "Private permanent conversation/evidence bucket. Defaults to a project-specific name."
  type        = string
  default     = null
}

variable "backup_bucket_name" {
  description = "Private rotating operational backup bucket. Defaults to a project-specific name."
  type        = string
  default     = null
}

variable "release_bucket_name" {
  description = "Private pinned service release and worker host-key bucket. Defaults to a project-specific name."
  type        = string
  default     = null
}

variable "backup_retention_days" {
  description = "Optional age in days after which rotating backup objects are deleted. Set null to keep all backup objects."
  type        = number
  default     = 30
  nullable    = true

  validation {
    condition     = var.backup_retention_days == null || (var.backup_retention_days >= 1 && var.backup_retention_days == floor(var.backup_retention_days))
    error_message = "backup_retention_days must be null or a whole number of at least 1."
  }
}

variable "snapshot_retention_days" {
  description = "Retention for daily persistent-disk snapshots."
  type        = number
  default     = 14

  validation {
    condition     = var.snapshot_retention_days >= 1 && var.snapshot_retention_days == floor(var.snapshot_retention_days)
    error_message = "snapshot_retention_days must be a positive whole number."
  }
}

variable "billing_account_id" {
  description = "Existing GCP billing account ID for the required budget alert."
  type        = string

  validation {
    condition     = length(trimspace(var.billing_account_id)) > 0
    error_message = "billing_account_id must be set to the existing billing account."
  }
}

variable "budget_amount_usd" {
  description = "Monthly pilot budget threshold in USD. Choose this after reviewing current pricing and available credits."
  type        = number

  validation {
    condition     = var.budget_amount_usd > 0 && var.budget_amount_usd * 100 == floor(var.budget_amount_usd * 100)
    error_message = "budget_amount_usd must be a positive amount with no more than two decimal places."
  }
}

variable "notification_emails" {
  description = "Verified email recipients for budget and infrastructure alerts."
  type        = set(string)

  validation {
    condition     = length(var.notification_emails) > 0 && alltrue([for email in var.notification_emails : can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", email))])
    error_message = "notification_emails must contain at least one email address."
  }
}

variable "iap_operator_emails" {
  description = "Google account emails granted IAP tunnel and OS Login access to the coordinator."
  type        = set(string)
  default     = []

  validation {
    condition     = alltrue([for email in var.iap_operator_emails : can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", email))])
    error_message = "iap_operator_emails must contain valid email addresses."
  }

  validation {
    condition     = !var.compute_enabled || length(var.iap_operator_emails) > 0
    error_message = "At least one IAP/OS Login operator must be configured while compute_enabled is true."
  }
}

variable "enable_https_iap" {
  description = "Optional public HTTPS load balancer protected by Google-managed IAP OAuth. Requires a verified host and an organization-owned project."
  type        = bool
  default     = false

  validation {
    condition = !var.enable_https_iap || (
      var.viewer_hostname != null &&
      can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$", var.viewer_hostname)) &&
      (length(var.iap_viewer_emails) + length(var.iap_viewer_domains)) > 0 &&
      var.iap_google_managed_oauth_confirmed
    )
    error_message = "enable_https_iap requires a DNS hostname, at least one viewer email or managed domain and confirmation that Google-managed OAuth is supported by the project organization."
  }
}

variable "coordinator_workflow" {
  description = "pilot preserves the idle deployment; linear requires an operator-installed protected /etc/factory/workflows/<service_revision>.md."
  type        = string
  default     = "pilot"
  validation {
    condition     = contains(["pilot", "linear"], var.coordinator_workflow)
    error_message = "coordinator_workflow must be pilot or linear."
  }
}

variable "coordinator_secret_env" {
  description = "Allowed coordinator credential environment names mapped to optional integration secret keys. Values are never in Terraform."
  type        = map(string)
  default     = {}
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
  description = "Opt-in signed webhook backend on port 8081, separate from dashboard IAP."
  type        = bool
  default     = false
  validation {
    condition     = !var.enable_linear_webhook || (var.enable_https_iap && var.coordinator_workflow == "linear")
    error_message = "The public webhook requires the HTTPS ingress and linear coordinator workflow."
  }
}

variable "viewer_hostname" {
  description = "DNS hostname for the optional HTTPS IAP ingress."
  type        = string
  default     = null
  nullable    = true
}

variable "iap_viewer_emails" {
  description = "Google account emails allowed through the optional IAP-protected HTTPS ingress."
  type        = set(string)
  default     = []

  validation {
    condition     = alltrue([for email in var.iap_viewer_emails : can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", email))])
    error_message = "iap_viewer_emails must contain valid email addresses."
  }
}

variable "iap_viewer_domains" {
  description = "Google Workspace or Cloud Identity managed domains allowed through dashboard IAP. Verify domain and project organization eligibility before deployment."
  type        = set(string)
  default     = []

  validation {
    condition     = alltrue([for domain in var.iap_viewer_domains : can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$", domain))])
    error_message = "iap_viewer_domains must contain bare lowercase DNS domains, without wildcards, email addresses or principal prefixes."
  }
}

variable "iap_google_managed_oauth_confirmed" {
  description = "Set true only after confirming this GCP project belongs to a Google organization; Google-managed IAP OAuth is organization-only."
  type        = bool
  default     = false
}
