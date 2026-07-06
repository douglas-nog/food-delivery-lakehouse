variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "project" {
  description = "Project name, used as resource prefix"
  type        = string
  default     = "fooddelivery"
}

variable "environment" {
  description = "Environment name (dev, etc.)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "brazilsouth"
}

variable "pg_admin_login" {
  description = "PostgreSQL administrator login"
  type        = string
  default     = "pgadmin"
}

variable "pg_admin_password" {
  description = "PostgreSQL administrator password"
  type        = string
  sensitive   = true
}

variable "my_ip" {
  description = "Developer public IPv4 for administrative access (injected at apply time, never committed)"
  type        = string
}
