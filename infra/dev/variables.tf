variable "minio_server" {
  type = string
  description = "The server to be used on MinIO operations"
  default = "localhost:9050"
}

variable "minio_user" {
  default = "datalake"
  type = string
  description = "IAM Username"
}

variable "minio_password" {
  sensitive = true
  default = "datalake"
  type = string
  description = "IAM Password"
}


variable "minio_ssl" {
  type = string
  default = false
}

variable "project_name" {
  default = "project-covid"
  type = string
}