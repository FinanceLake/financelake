# ============================================================================
# Development Environment Configuration
# ============================================================================
# This file contains development-specific variable values for FinanceLake
# ============================================================================

# Environment settings
environment = "dev"
project_name = "financelake"

# AWS settings
aws_region = "us-east-1"

# Network settings (smaller CIDR for dev)
vpc_cidr = "10.10.0.0/16"
private_subnets = ["10.10.1.0/24", "10.10.2.0/24", "10.10.3.0/24"]
public_subnets = ["10.10.101.0/24", "10.10.102.0/24", "10.10.103.0/24"]

# EKS settings (smaller cluster for dev)
eks_version = "1.28"

# Instance types (cost-optimized for dev)
data_processing_instance_types = ["t3.medium", "t3.large"]
ml_training_instance_types = ["t3.medium", "t3.large"]
services_instance_types = ["t3.small", "t3.medium"]

# Monitoring (basic monitoring for dev)
enable_monitoring = true
enable_logging = true
log_retention_days = 7

# Security (relaxed for dev)
enable_encryption = true
enable_backup = false

# Cost optimization
enable_cost_allocation_tags = true
enable_spot_instances = false

# Compliance (none for dev)
compliance_framework = "none"

# Additional tags
additional_tags = {
  "CostCenter" = "engineering"
  "Project" = "financelake-dev"
  "Backup" = "daily"
}

# Feature flags
enable_experimental_features = true
enable_beta_services = false
