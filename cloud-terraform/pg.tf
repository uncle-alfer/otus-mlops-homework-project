module "mlflow-db" {
  source = "github.com/terraform-yc-modules/terraform-yc-postgresql.git"

  network_id  = "enpc088i7mdlm4ocijh6"
  name        = "mlflow-db"
  description = "Single-node PostgreSQL cluster for mlops project"

  maintenance_window = {
    type = "WEEKLY"
    day  = "SUN"
    hour = "02"
  }

  access_policy = {
    web_sql = true
  }

  performance_diagnostics = {
    enabled = true
  }

  hosts_definition = [
    {
      zone             = "ru-central1-a"
      assign_public_ip = true
      subnet_id        = "e9bbjfj1bl3choicj6fh"
    }
  ]

  postgresql_config = {
    max_connections                = 395
    enable_parallel_hash           = true
    autovacuum_vacuum_scale_factor = 0.34
    default_transaction_isolation  = "TRANSACTION_ISOLATION_READ_COMMITTED"
    shared_preload_libraries       = "SHARED_PRELOAD_LIBRARIES_AUTO_EXPLAIN,SHARED_PRELOAD_LIBRARIES_PG_HINT_PLAN"
  }

  default_user_settings = {
    default_transaction_isolation = "read committed"
    log_min_duration_statement    = 5000
  }

  databases = [
    {
      name       = "mlflow"
      owner      = "mlflow"
      lc_collate = "ru_RU.UTF-8"
      lc_type    = "ru_RU.UTF-8"
      extensions = ["uuid-ossp", "xml2"]
    }
  ]

  owners = [
    {
      name       = "mlflow"
      password   = "mlflow-pass"
      conn_limit = 15
    }
  ]

  # users = [
  #   {
  #     name        = "mlflow"
  #     password    = "mlflmlflow-passow"
  #     conn_limit  = 30
  #     permissions = ["mlflow"]
  #     settings = {
  #       pool_mode                   = "transaction"
  #       prepared_statements_pooling = true
  #     }
  #   }
  # ]
}

output "postgresql_cluster_id" {
  description = "PostgreSQL cluster ID"
  value       = try(module.mlflow-db.cluster_id, null)
}

output "postgresql_cluster_name" {
  description = "PostgreSQL cluster name"
  value       = try(module.mlflow-db.cluster_name, null)
}

output "postgresql_cluster_host_names_list" {
  description = "PostgreSQL cluster host name list"
  value       = try(module.mlflow-db.cluster_host_names_list, null)
}

output "postgresql_cluster_fqdns_list" {
  description = "PostgreSQL cluster FQDNs list"
  value       = try(module.mlflow-db.cluster_fqdns_list, null)
}

output "mlflow-db_owners" {
  description = "A list of DB owners users with password."
  sensitive   = true
  value       = try(module.mlflow-db.owners_data, null)
}

output "mlflow-db_users" {
  description = "A list of separate DB users with passwords."
  sensitive   = true
  value       = try(module.mlflow-db.users_data, null)
}

output "postgresql_databases" {
  description = "A list of database names."
  value       = try(module.mlflow-db.databases, null)
}

output "postgresql_connection_step_1" {
  description = "1 step - Install certificate"
  value       = try(module.mlflow-db.connection_step_1, null)
}
output "postgresql_connection_step_2" {
  description = "2 step - Execute psql command for a connection to the cluster"
  value       = try(module.mlflow-db.connection_step_2, null)
}