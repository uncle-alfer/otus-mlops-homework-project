terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  zone = "ru-central1-a"
}

module "kube" {
  source     = "github.com/terraform-yc-modules/terraform-yc-kubernetes.git"
  network_id = "enpc088i7mdlm4ocijh6"

  master_locations = [
    {
      zone      = "ru-central1-a",
      subnet_id = "e9bbjfj1bl3choicj6fh"
    },
    {
      zone      = "ru-central1-b",
      subnet_id = "e2l7sl7mdpedcilftljr"
    },
    {
      zone      = "ru-central1-d",
      subnet_id = "fl8g90as6093dftqdm98"
    }
  ]

  master_maintenance_windows = [
    {
      day        = "monday"
      start_time = "23:00"
      duration   = "3h"
    }
  ]

  node_groups = {
    "yc-k8s-ng-01" = {
      description = "Kubernetes nodes group 01"
      fixed_scale = {
        size = 1
      }
      node_labels = {
        role        = "worker-01"
        environment = "testing"
      }
    },
  }
}