locals {
  data_lake_bucket = "properties_lake"
  all_project_services = concat(var.gcp_service_list, [
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
  ])
}

variable "project_id" {
  description = "ct-properties"
  type = string
}

variable "region" {
  description = "Regional location of GCP resources. Choose based on your location: https://cloud.google.com/about/locations"
  default = "us-east4"
  type = string
}

variable "gcp_storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "staging_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "stg_properties_dataset"
}

variable "production_dataset_name" {
  description = "BigQuery Dataset that transformed data (from DBT) will be written to"
  type = string
  default = "prod_properties_dataset"
}

variable "instance_name" {
  type = string
  default = "ct-properties-instance"
}

variable "machine_type" {
  type = string
  default = "e2-standard-2"
}

variable "zone" {
  description = "Region for VM"
  type = string
  default = "us-east4-a"
}

variable "gce_ssh_user" {
  default = "carlos"   # adjust to preference
}

variable "ssh_pub_key_file" {
  description = "Path to the generated SSH public key on your local machine"
  default = "~/.ssh/gcloud.pub"  # adjust accordingly
}

variable "ssh_priv_key_file" {
  description = "Path to the generated SSH private key on your local machine"
  default = "~/.ssh/gcloud"     # adjust accordingly
}

variable "gcp_service_list" {
  description = "The list of apis necessary for the project"
  type        = list(string)
  default     = ["storage.googleapis.com",]
}

variable "account_id" {
  description = "The service account ID."  # Changing this forces a new service account to be created."
  default =  "ct-properties-sa"
}

variable "description" {
  description = "Custom SA for VM instance." # Can be updated without creating a new resource
  default     = "managed-by-terraform"
}

variable "roles" {
  type        = list(string)
  description = "The roles that will be granted to the service account."
  default     = ["roles/owner","roles/storage.admin","roles/storage.objectAdmin","roles/bigquery.admin"]
}