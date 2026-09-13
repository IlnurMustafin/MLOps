# airflow.tf
# Минимальный конфиг для запуска Managed Airflow в Yandex Cloud

terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
}

provider "yandex" {
  service_account_key_file = "key.json"
  cloud_id                 = "b1gm14ai9dbs9qnl2qjp"
  folder_id                = "b1g6v0u6dj5boh3nd6vl"
  zone                     = "ru-central1-a"
}

# 1. Сеть и подсеть (нужны для Airflow)

resource "yandex_vpc_network" "airflow_network" {
  name = "airflow-network"
}

resource "yandex_vpc_subnet" "airflow_subnet" {
  name           = "airflow-subnet-a"
  zone           = "ru-central1-a"
  network_id     = yandex_vpc_network.airflow_network.id
  v4_cidr_blocks = ["10.10.1.0/24"]
  route_table_id = yandex_vpc_route_table.nat_route_table.id
}

# 2. Сервисный аккаунт для Airflow

resource "yandex_iam_service_account" "airflow_sa" {
  name = "airflow-sa"
}

# Роли, необходимые Airflow для работы
resource "yandex_resourcemanager_folder_iam_member" "airflow_roles" {
  for_each = toset([
    "storage.viewer",                       # Чтение DAG-файлов из S3
    "managed-airflow.integrationProvider",  # Интеграция с другими сервисами
    "vpc.user",                             # Использование сети
    "logging.writer",      # <-- Добавлено
    "monitoring.editor",   # <-- Добавлено
  ])
  folder_id = "b1g6v0u6dj5boh3nd6vl"
  role      = each.value
  member    = "serviceAccount:${yandex_iam_service_account.airflow_sa.id}"
}

# 3. Бакет для DAG-файлов

resource "yandex_storage_bucket" "airflow_dags" {
  bucket = "airflow-dags-${yandex_iam_service_account.airflow_sa.id}"
  acl    = "private"
}

# 4. Сам кластер Airflow

resource "yandex_airflow_cluster" "airflow" {
  name           = "test-airflow"
  folder_id      = "b1g6v0u6dj5boh3nd6vl"
  admin_password = "OtusAirflow2026!"  # Придумай свой сложный пароль

  subnet_ids         = [yandex_vpc_subnet.airflow_subnet.id]
  service_account_id = yandex_iam_service_account.airflow_sa.id

  # Синхронизация DAG-файлов из S3
  code_sync = {
    s3 = {
      bucket = yandex_storage_bucket.airflow_dags.bucket
    }
  }

  # Добавляем DAG Processor (обязательно для Airflow 3.x)
  dag_processor = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  # Минимальные ресурсы (для теста)
  webserver = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  scheduler = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  worker = {
    min_count          = 1
    max_count          = 1
    resource_preset_id = "c1-m4"
  }

  # Логи (опционально, но полезно)
  logging = {
    enabled   = true
    folder_id = "b1g6v0u6dj5boh3nd6vl"
    min_level = "INFO"
  }
}

# 5. Вывод ссылки на Airflow UI

output "airflow_ui_url" {
  value       = "https://${yandex_airflow_cluster.airflow.id}.airflow.yandexcloud.net"
  description = "Ссылка на веб-интерфейс Airflow (логин: admin)"
}

# 6.  NAT-шлюз для доступа в интернет
resource "yandex_vpc_gateway" "nat_gateway" {
  name = "airflow-nat-gateway"
  shared_egress_gateway {}
}

# Таблица маршрутизации через NAT
resource "yandex_vpc_route_table" "nat_route_table" {
  name       = "airflow-nat-route-table"
  network_id = yandex_vpc_network.airflow_network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}