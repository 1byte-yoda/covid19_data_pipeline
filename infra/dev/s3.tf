resource "minio_s3_bucket" "dev_s3_bucket" {
  bucket = "dev-${var.project_name}"
  acl    = "public"
}

output "bucket_name" {
  value = minio_s3_bucket.dev_s3_bucket.id
}