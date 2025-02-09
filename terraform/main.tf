terraform {
  required_version = ">= 1.0"

  backend "s3" {
    region = "us-west-2"
    bucket = "ebird-etl"
    key    = "ebird-terraform-state/ebird-etl.tfstate"
  }
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}
