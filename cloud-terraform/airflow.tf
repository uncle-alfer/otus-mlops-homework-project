resource "yandex_compute_disk" "boot-disk-airflow" {
  name     = "boot-disk-airflow"
  type     = "network-ssd"
  zone     = "ru-central1-a"
  size     = "60"
  image_id = "fd83s8u085j3mq231ago"
}

resource "yandex_compute_instance" "airflow" {
  name = "airflow"

  resources {
    cores  = 6
    memory = 12
  }

  boot_disk {
    disk_id = yandex_compute_disk.boot-disk-airflow.id
  }

  network_interface {
    subnet_id = "e9bbjfj1bl3choicj6fh"
    nat       = true
  }

  metadata = {
    user-data = "${file("/home/terraform/otus-mlops-homework-project/cloud-terraform/airflow-user.txt")}"
  }
}

output "internal_ip_address_airflow" {
  value = yandex_compute_instance.airflow.network_interface.0.ip_address
}

output "external_ip_address_airflow" {
  value = yandex_compute_instance.airflow.network_interface.0.nat_ip_address
}

# generate inventory file for Ansible
resource "local_file" "hosts_cfg" {
  content = templatefile("/home/terraform/otus-mlops-homework-project/cloud-terraform/templates/airflow_hosts.tpl",
    {
      airflow_ip = yandex_compute_instance.airflow.network_interface.0.nat_ip_address
    }
  )
  filename = "../ansible/inventory/airflow_hosts.cfg"
}