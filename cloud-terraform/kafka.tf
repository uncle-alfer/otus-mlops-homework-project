resource "yandex_mdb_kafka_cluster" "kafka" {
  environment         = "PRODUCTION"
  name                = "kafka"
  network_id          = "enpc088i7mdlm4ocijh6"
  subnet_ids          = ["e9bbjfj1bl3choicj6fh"]
  security_group_ids  = ["enpvb37su7rtfdoh8m0o"]
  deletion_protection = false

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

resource "yandex_mdb_kafka_user" "kafka-producer" {
  cluster_id = yandex_mdb_kafka_cluster.kafka.id
  name       = "kafka-producer"
  password   = "kafka-producer-pass"
  permission {
    topic_name = "input"
    role       = "ACCESS_ROLE_PRODUCER"
  }
  permission {
    topic_name = "output"
    role       = "ACCESS_ROLE_PRODUCER"
  }
}

resource "yandex_mdb_kafka_user" "kafka-consumer" {
  cluster_id = yandex_mdb_kafka_cluster.kafka.id
  name       = "kafka-consumer"
  password   = "kafka-consumer-pass"
  permission {
    topic_name = "input"
    role       = "ACCESS_ROLE_CONSUMER"
  }
  permission {
    topic_name = "output"
    role       = "ACCESS_ROLE_CONSUMER"
  }
}

resource "yandex_mdb_kafka_topic" "input" {
  depends_on         = [yandex_mdb_kafka_cluster.kafka]
  cluster_id         = yandex_mdb_kafka_cluster.kafka.id
  name               = "input"
  partitions         = 6
  replication_factor = 1
  topic_config {}
}

resource "yandex_mdb_kafka_topic" "output" {
  depends_on         = [yandex_mdb_kafka_cluster.kafka]
  cluster_id         = yandex_mdb_kafka_cluster.kafka.id
  name               = "output"
  partitions         = 6
  replication_factor = 1
  topic_config {}
}