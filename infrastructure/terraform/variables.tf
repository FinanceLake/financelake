# ============================================================================
# FinanceLake - Terraform Variables
# ============================================================================
# Enterprise-grade variable definitions with validation and documentation
# ============================================================================

# ============================================================================
# PROJECT CONFIGURATION
# ============================================================================

variable "project_name" {
  description = "Name of the project - used for resource naming"
  type        = string
  default     = "financelake"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "Project name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "team_name" {
  description = "Team name for resource tagging and organization"
  type        = string
  default     = "data-engineering"
}

# ============================================================================
# AWS CONFIGURATION
# ============================================================================

variable "aws_region" {
  description = "AWS region for resource deployment"
  type        = string
  default     = "us-east-1"

  validation {
    condition = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.aws_region))
    error_message = "AWS region must be a valid region identifier (e.g., us-east-1)."
  }
}

# ============================================================================
# NETWORKING
# ============================================================================

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "VPC CIDR must be a valid CIDR block."
  }
}

variable "private_subnets" {
  description = "Private subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]

  validation {
    condition = alltrue([
      for subnet in var.private_subnets : can(cidrhost(subnet, 0))
    ])
    error_message = "All private subnets must be valid CIDR blocks."
  }
}

variable "public_subnets" {
  description = "Public subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  validation {
    condition = alltrue([
      for subnet in var.public_subnets : can(cidrhost(subnet, 0))
    ])
    error_message = "All public subnets must be valid CIDR blocks."
  }
}

# ============================================================================
# EKS CONFIGURATION
# ============================================================================

variable "eks_version" {
  description = "Kubernetes version for EKS cluster"
  type        = string
  default     = "1.28"

  validation {
    condition     = can(regex("^1\\.[0-9]{2}$", var.eks_version))
    error_message = "EKS version must be in format 1.xx (e.g., 1.28)."
  }
}

# ============================================================================
# NODE GROUP CONFIGURATION
# ============================================================================

variable "data_processing_instance_types" {
  description = "EC2 instance types for data processing workloads"
  type        = list(string)
  default     = ["m6i.2xlarge", "m6i.4xlarge", "m6i.8xlarge"]

  validation {
    condition = alltrue([
      for instance_type in var.data_processing_instance_types :
      can(regex("^[a-z][0-9][a-z]*\\.[0-9]*xlarge$", instance_type))
    ])
    error_message = "Instance types must be valid EC2 instance types."
  }
}

variable "ml_training_instance_types" {
  description = "EC2 instance types for ML training workloads"
  type        = list(string)
  default     = ["p3.2xlarge", "p3.8xlarge", "p4d.24xlarge"]

  validation {
    condition = alltrue([
      for instance_type in var.ml_training_instance_types :
      can(regex("^[a-z][0-9][a-z]*\\.[0-9]*xlarge$", instance_type))
    ])
    error_message = "Instance types must be valid EC2 instance types."
  }
}

variable "services_instance_types" {
  description = "EC2 instance types for general services"
  type        = list(string)
  default     = ["t3.medium", "t3.large", "t3.xlarge"]

  validation {
    condition = alltrue([
      for instance_type in var.services_instance_types :
      can(regex("^[a-z][0-9][a-z]*\\.[0-9]*xlarge$|^t3\\.(nano|micro|small|medium|large|xlarge)$", instance_type))
    ])
    error_message = "Instance types must be valid EC2 instance types."
  }
}

# ============================================================================
# MONITORING AND LOGGING
# ============================================================================

variable "enable_monitoring" {
  description = "Enable comprehensive monitoring stack (Prometheus, Grafana, ELK)"
  type        = bool
  default     = true
}

variable "enable_logging" {
  description = "Enable centralized logging with ELK stack"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30

  validation {
    condition     = var.log_retention_days >= 1 && var.log_retention_days <= 365
    error_message = "Log retention must be between 1 and 365 days."
  }
}

# ============================================================================
# SECURITY
# ============================================================================

variable "enable_encryption" {
  description = "Enable encryption at rest for all resources"
  type        = bool
  default     = true
}

variable "enable_backup" {
  description = "Enable automated backups for critical resources"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 7

  validation {
    condition     = var.backup_retention_days >= 1 && var.backup_retention_days <= 365
    error_message = "Backup retention must be between 1 and 365 days."
  }
}

# ============================================================================
# COST OPTIMIZATION
# ============================================================================

variable "enable_cost_allocation_tags" {
  description = "Enable cost allocation tags for resource tracking"
  type        = bool
  default     = true
}

variable "enable_spot_instances" {
  description = "Use spot instances for non-critical workloads to reduce costs"
  type        = bool
  default     = false
}

# ============================================================================
# COMPLIANCE
# ============================================================================

variable "compliance_framework" {
  description = "Compliance framework to apply (soc2, pci-dss, hipaa, none)"
  type        = string
  default     = "none"

  validation {
    condition     = contains(["soc2", "pci-dss", "hipaa", "gdpr", "none"], var.compliance_framework)
    error_message = "Compliance framework must be one of: soc2, pci-dss, hipaa, gdpr, none."
  }
}

# ============================================================================
# TAGS
# ============================================================================

variable "additional_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for key, value in var.additional_tags :
      can(regex("^[a-zA-Z0-9_-]+$", key)) && length(value) <= 256
    ])
    error_message = "Tag keys must contain only alphanumeric characters, hyphens, and underscores. Tag values must be 256 characters or less."
  }
}

# ============================================================================
# FEATURE FLAGS
# ============================================================================

variable "enable_experimental_features" {
  description = "Enable experimental features (not recommended for production)"
  type        = bool
  default     = false
}

variable "enable_beta_services" {
  description = "Enable beta AWS services and features"
  type        = bool
  default     = false
}
