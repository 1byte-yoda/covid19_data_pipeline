terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "3.5.0"
    }
  }
}

provider "minio" {
  minio_server   = var.minio_server
  minio_user     = var.minio_user
  minio_password = var.minio_password
  minio_ssl      = var.minio_ssl
}