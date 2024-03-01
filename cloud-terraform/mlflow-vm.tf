resource "yandex_compute_disk" "boot-disk-mlflow" {
  name     = "boot-disk-mlflow"
  type     = "network-ssd"
  zone     = "ru-central1-a"
  size     = "20"
  image_id = "fd83s8u085j3mq231ago"
}

resource "yandex_compute_instance" "mlflow" {
  name = "mlflow"

  resources {
    cores  = 2
    memory = 4
  }

  boot_disk {
    disk_id = yandex_compute_disk.boot-disk-mlflow.id
  }

  network_interface {
    subnet_id = "e9bbjfj1bl3choicj6fh"
    nat       = true
  }

  metadata = {
    user-data = "${file("/home/terraform/otus-mlops-homework-project/cloud-terraform/mlflow-user.txt")}"
  }
}

output "internal_ip_address_mlflow" {
  value = yandex_compute_instance.mlflow.network_interface.0.ip_address
}

output "external_ip_address_mlflow" {
  value = yandex_compute_instance.mlflow.network_interface.0.nat_ip_address
}

# generate inventory file for Ansible
resource "local_file" "mlflow_hosts_cfg" {
  content = templatefile("/home/terraform/otus-mlops-homework-project/cloud-terraform/templates/mlflow_hosts.tpl",
    {
      mlflow_ip = yandex_compute_instance.mlflow.network_interface.0.nat_ip_address
    }
  )
  filename = "../ansible/inventory/mlflow_hosts.cfg"
}