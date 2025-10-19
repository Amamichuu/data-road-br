### Variables
## Environment
# Env
variable "env" {
  description = "Environment name (dev, prod)"
  type        = string
  default     = "dev"
}

## LocalStack
# Endpoint
variable "local_stack_endpoint" {
  description = "LocalStack Endpoint"
  type        = string
  default     = "http://localhost:4566"
}

## AWS
# AWS Access Key
variable "aws_access_key" {
  description = "AWS access key for dev"
  type        = string
  default     = "test"
}

# AWS Secret Key
variable "aws_secret_access_key" {
  description = "AWS secret access key for dev"
  type        = string
  default     = "test"
}

# AWS Region
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-west-2"
}