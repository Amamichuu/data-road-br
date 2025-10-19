### Providers
## AWS
# LocalStack
provider "aws" {
  alias      = "localstack"
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_access_key

  s3_use_path_style           = true
  skip_requesting_account_id  = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true

  endpoints {
    s3 = var.local_stack_endpoint
  }
}