# ============================================================================
# FinanceLake - Infrastructure as Code
# ============================================================================
# Enterprise-grade infrastructure for real-time financial analytics platform
# ============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}

# ============================================================================
# PROVIDERS
# ============================================================================

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = var.team_name
    }
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.cluster.token
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    token                  = data.aws_eks_cluster_auth.cluster.token
  }
}

# ============================================================================
# DATA SOURCES
# ============================================================================

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_name
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ============================================================================
# NETWORKING
# ============================================================================

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = "${var.project_name}-vpc"
  cidr = var.vpc_cidr

  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = var.private_subnets
  public_subnets  = var.public_subnets

  enable_nat_gateway     = true
  single_nat_gateway     = var.environment == "dev"
  enable_dns_hostnames   = true
  enable_dns_support     = true

  # VPC Flow Logs for security monitoring
  enable_flow_log                      = true
  create_flow_log_cloudwatch_log_group = true
  create_flow_log_cloudwatch_iam_role  = true

  tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }

  public_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                      = "1"
  }

  private_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"             = "1"
  }
}

# ============================================================================
# SECURITY GROUPS
# ============================================================================

resource "aws_security_group" "data_lake" {
  name_prefix = "${var.project_name}-data-lake-"
  vpc_id      = module.vpc.vpc_id

  # Kafka ports
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
    description = "Kafka broker access"
  }

  # Flink ports
  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
    description = "Flink job manager"
  }

  # Iceberg catalog
  ingress {
    from_port   = 8181
    to_port     = 8181
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
    description = "Iceberg REST catalog"
  }

  # API Gateway
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
    description = "API Gateway"
  }

  # Frontend
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
    description = "React Frontend"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-data-lake-sg"
  }
}

# ============================================================================
# EKS CLUSTER
# ============================================================================

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name    = local.cluster_name
  cluster_version = var.eks_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access  = var.environment == "dev"
  cluster_endpoint_private_access = true

  # Encryption at rest
  cluster_encryption_config = [
    {
      provider_key_arn = aws_kms_key.eks.arn
      resources        = ["secrets"]
    }
  ]

  # CloudWatch logging
  cluster_enabled_log_types = [
    "api", "audit", "authenticator", "controllerManager", "scheduler"
  ]

  # Node groups for different workloads
  eks_managed_node_groups = {
    # High-memory nodes for data processing
    data_processing = {
      name           = "data-processing"
      instance_types = var.data_processing_instance_types
      min_size       = var.environment == "prod" ? 3 : 1
      max_size       = var.environment == "prod" ? 10 : 3
      desired_size   = var.environment == "prod" ? 5 : 1

      labels = {
        workload = "data-processing"
        team     = var.team_name
      }

      taints = [{
        key    = "dedicated"
        value  = "data-processing"
        effect = "NO_SCHEDULE"
      }]

      tags = {
        "k8s.io/cluster-autoscaler/enabled" = "true"
      }
    }

    # CPU-optimized nodes for ML training
    ml_training = {
      name           = "ml-training"
      instance_types = var.ml_training_instance_types
      min_size       = 0
      max_size       = var.environment == "prod" ? 5 : 2
      desired_size   = 0

      labels = {
        workload = "ml-training"
        team     = var.team_name
      }

      taints = [{
        key    = "dedicated"
        value  = "ml-training"
        effect = "NO_SCHEDULE"
      }]

      tags = {
        "k8s.io/cluster-autoscaler/enabled" = "true"
      }
    }

    # General purpose nodes for services
    services = {
      name           = "services"
      instance_types = var.services_instance_types
      min_size       = 2
      max_size       = var.environment == "prod" ? 20 : 5
      desired_size   = var.environment == "prod" ? 5 : 2

      labels = {
        workload = "services"
        team     = var.team_name
      }
    }
  }

  # Fargate for serverless workloads (optional)
  fargate_profiles = {
    default = {
      name = "default"
      selectors = [
        {
          namespace = "kube-system"
        },
        {
          namespace = "default"
        }
      ]
    }
  }

  # IAM roles for service accounts (IRSA)
  enable_irsa = true

  # Cluster autoscaler
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }
}

# ============================================================================
# KMS KEYS
# ============================================================================

resource "aws_kms_key" "eks" {
  description             = "EKS Secret Encryption Key"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "${var.project_name}-eks-key"
  }
}

resource "aws_kms_key" "s3" {
  description             = "S3 Bucket Encryption Key"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "${var.project_name}-s3-key"
  }
}

# ============================================================================
# S3 BUCKETS (Data Lake Storage)
# ============================================================================

module "s3_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 3.0"

  bucket = "${var.project_name}-data-lake-${var.environment}"

  # Server-side encryption
  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = aws_kms_key.s3.arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  # Versioning
  versioning = {
    enabled = true
  }

  # Lifecycle rules for cost optimization
  lifecycle_rule = [
    {
      id     = "data_lake_lifecycle"
      status = "Enabled"

      # Move old data to cheaper storage
      transition = [
        {
          days          = 30
          storage_class = "STANDARD_IA"
        },
        {
          days          = 90
          storage_class = "GLACIER"
        }
      ]

      # Delete incomplete multipart uploads
      abort_incomplete_multipart_upload_days = 7
    }
  ]

  # Public access block
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  # Access logging
  logging = {
    target_bucket = module.s3_access_logs.s3_bucket_id
    target_prefix = "access-logs/"
  }
}

# Access logs bucket
module "s3_access_logs" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 3.0"

  bucket = "${var.project_name}-access-logs-${var.environment}"

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  versioning = {
    enabled = true
  }
}

# ============================================================================
# IAM ROLES AND POLICIES
# ============================================================================

# IAM role for EKS cluster autoscaler
module "cluster_autoscaler_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${var.project_name}-cluster-autoscaler"

  attach_cluster_autoscaler_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:cluster-autoscaler"]
    }
  }
}

# IAM role for data processing services
module "data_processing_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${var.project_name}-data-processing"

  role_policy_arns = {
    s3_access = aws_iam_policy.data_processing_s3.arn
    kms_access = aws_iam_policy.data_processing_kms.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = [
        "data:flink-jobmanager",
        "data:spark-driver",
        "processing:default"
      ]
    }
  }
}

# Custom IAM policies
resource "aws_iam_policy" "data_processing_s3" {
  name        = "${var.project_name}-data-processing-s3"
  description = "S3 access for data processing workloads"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          module.s3_bucket.s3_bucket_arn,
          "${module.s3_bucket.s3_bucket_arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "data_processing_kms" {
  name        = "${var.project_name}-data-processing-kms"
  description = "KMS access for data processing workloads"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:ReEncrypt*"
        ]
        Resource = aws_kms_key.s3.arn
      }
    ]
  })
}

# ============================================================================
# KUBERNETES ADD-ONS
# ============================================================================

# Cluster autoscaler
resource "helm_release" "cluster_autoscaler" {
  name       = "cluster-autoscaler"
  repository = "https://kubernetes.github.io/autoscaler"
  chart      = "cluster-autoscaler"
  namespace  = "kube-system"
  version    = "9.29.0"

  set {
    name  = "autoDiscovery.clusterName"
    value = module.eks.cluster_name
  }

  set {
    name  = "awsRegion"
    value = var.aws_region
  }

  set {
    name  = "rbac.serviceAccount.create"
    value = "true"
  }

  set {
    name  = "rbac.serviceAccount.name"
    value = "cluster-autoscaler"
  }

  set {
    name  = "rbac.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = module.cluster_autoscaler_irsa.iam_role_arn
  }
}

# Metrics server for HPA
resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  namespace  = "kube-system"
  version    = "3.11.0"
}

# AWS Load Balancer Controller
resource "helm_release" "aws_load_balancer_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  namespace  = "kube-system"
  version    = "1.6.2"

  set {
    name  = "clusterName"
    value = module.eks.cluster_name
  }

  set {
    name  = "serviceAccount.create"
    value = "false"
  }

  set {
    name  = "serviceAccount.name"
    value = "aws-load-balancer-controller"
  }
}

# ============================================================================
# LOCALS
# ============================================================================

locals {
  cluster_name = "${var.project_name}-eks-${var.environment}"
}

# ============================================================================
# OUTPUTS
# ============================================================================

output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "s3_bucket_name" {
  description = "Data lake S3 bucket name"
  value       = module.s3_bucket.s3_bucket_id
}

output "s3_bucket_arn" {
  description = "Data lake S3 bucket ARN"
  value       = module.s3_bucket.s3_bucket_arn
}
