resource "yandex_mdb_kafka_cluster" "kafka" {
  environment         = "PRODUCTION"
  name                = "kafka"
  network_id          = "enpc088i7mdlm4ocijh6"
  subnet_ids          = [ "e9bbjfj1bl3choicj6fh" ]
  security_group_ids  = [ "enpvb37su7rtfdoh8m0o" ]
  deletion_protection = true

  config {
    assign_public_ip = true
    brokers_count    = 1
    version          = "3.2"
    kafka {
      resources {
        disk_size          = 32
        disk_type_id       = "network-ssd"
        resource_preset_id = "s2.micro"
      }
      kafka_config {}
    }

    zones = [
      "ru-central1-a"
    ]
  }
}