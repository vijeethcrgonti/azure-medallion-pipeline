variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "synapse_admin_login" {
  description = "Synapse SQL admin username"
  type        = string
  sensitive   = true
}

variable "synapse_admin_password" {
  description = "Synapse SQL admin password"
  type        = string
  sensitive   = true
}
