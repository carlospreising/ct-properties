resource "google_compute_instance" "vm_instance" {
  name         = var.instance_name
  machine_type = var.machine_type
  zone         = var.zone
  project      = var.project_id

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    network    = "default"
    subnetwork = "default"
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email = google_service_account.ct-properties-sa.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    sshKeys = "${var.gce_ssh_user}:${file(var.ssh_pub_key_file)}"
  }

  provisioner "remote-exec" {
    connection {
      type        = "ssh"
      user        = var.gce_ssh_user
      host        = google_compute_instance.vm_instance.network_interface[0].access_config[0].nat_ip
      private_key = file(var.ssh_priv_key_file)
    }
    inline = [
        "sudo apt-get update -y",
        "sudo apt-get install git ca-certificates python3-pip -y",
        "cd ~/ && git clone https://github.com/carlospreising/ct-properties.git",
        "sudo apt-get install curl",
        "curl -fsSL https://get.docker.com -o get-docker.sh"
    ]
  }
}