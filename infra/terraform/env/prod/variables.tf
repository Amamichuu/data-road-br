### Variables
## Environment
# Env
variable "env" {
  description = "Environment name (dev, prod)"
  type        = string
  default     = "prod"
}

## AWS
# AWS Access Key
variable "aws_access_key" {
  description = "AWS access key for production"
  type        = string
  sensitive   = true
}

# AWS Secret Key
variable "aws_secret_access_key" {
  description = "AWS secret access key for production"
  type        = string
  sensitive   = true
}

# AWS Region
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-west-2"
}