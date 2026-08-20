## 1. Создание бакета в Yandex Cloud Object Storage с использованием Terraform

### Цель

Создать облачное хранилище (S3-совместимый бакет) для копирования и дальнейшей работы с данными транзакций.

### Результат

Бакет успешно создан в Yandex Cloud Object Storage.

| Параметр | Значение |
|----------|----------|
| **Имя бакета** | `otus-mlops-ilnur-data` |
| **Тип доступа** | `private` (приватный) |
| **Максимальный размер** | 100 ГБ |
| **Регион** | `ru-central1-a` |
| **S3 URI** | `s3://otus-mlops-ilnur-data` |
| **HTTP URL** | `https://storage.yandexcloud.net/otus-mlops-ilnur-data` |

> **Важно:** бакет создан в режиме `private`. Публичный доступ будет открыт на следующем шаге (пункт 2) для проверки преподавателем.

---

### Terraform-скрипты

Все файлы с инфраструктурой находятся в папке ***(/terraform)***.

**Структура:**

Папка terraform содержит следующие файлы:
1. provider.tf - Настройка провайдера Yandex Cloud
2. main.tf - Описание ресурса (бакет)
3. outputs.tf - Вывод информации о созданном ресурсе
4. variables.tf - Переменные
5. terraform.tfvars - Значения переменных (НЕ добавлен в Git)
6. key.json - Ключ сервисного аккаунта (НЕ добавлен в Git)


**Содержимое provider.tf:**
```
terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  service_account_key_file = "key.json"
  cloud_id                 = "b1gm14ai9dbs9qnl2qjp"
  folder_id                = "b1g6v0u6dj5boh3nd6vl"
  zone                     = "ru-central1-a"
}
```

**Содержимое main.tf:**
```
resource "yandex_storage_bucket" "my_bucket" {
  bucket   = "otus-mlops-ilnur-data"
  acl      = "private"
  max_size = 107374182400 # 100 ГБ
}
```

**Содержимое outputs.tf:**
```
output "bucket_name" {
  value = yandex_storage_bucket.my_bucket.bucket
  description = "Имя созданного бакета"
}

output "bucket_url" {
  value = "s3://${yandex_storage_bucket.my_bucket.bucket}"
  description = "S3 URL бакета"
}
```

**Команды для воспроизведения:**
```
cd terraform
terraform init
terraform plan
terraform apply
```

### Вывод по пункту 1

Бакет успешно создан с помощью Terraform. На следующем этапе он будет открыт для публичного доступа, чтобы преподаватель мог проверить скопированные данные.


## 2. Копирование данных и публичный доступ

### Цель

Скопировать данные из предоставленного хранилища `s3://otus-mlops-source-data/` в свой бакет `s3://otus-mlops-ilnur-data/` и открыть публичный доступ для проверки преподавателем.

---

### Выполненные действия

Для копирования данных использовался инструмент `s3cmd`. Команда:

```
s3cmd cp --acl-public --recursive s3://otus-mlops-source-data/ s3://otus-mlops-ilnur-data/
```

**Скопировано 5 файлов общим размером ~14.5 ГБ:**

```
2019-08-22.txt  (2.8 ГБ)
2019-09-21.txt  (2.85 ГБ)
2019-10-21.txt  (2.89 ГБ)
2019-11-20.txt  (2.94 ГБ)
2019-12-20.txt  (2.99 ГБ)
```
Флаг --acl-public при копировании автоматически сделал все файлы в бакете общедоступными для чтения.

**Публичная ссылка на один из скопированных файлов в бакете:**

```
https://storage.yandexcloud.net/otus-mlops-ilnur-data/2019-08-22.txt
```


## 3. Создание Spark-кластера в Yandex Data Processing

### Цель

Создать Spark-кластер в Yandex Cloud Data Processing для обработки данных о транзакциях. Кластер состоит из двух подкластеров: master (управляющий узел) и data (вычислительный узел). Для экономии ресурсов используется Terraform.

---

### Подготовка

#### 1. Создание SSH-ключа

Для подключения к мастер-узлу кластера по SSH была создан ключевая пара:

```
ssh-keygen -t rsa -b 4096 -C "ilnur@otus-mlops"
```

#### 2. Настройка сервисного аккаунта

Для управления кластером сервисному аккаунту s3-admin-sa была выдана роль dataproc.agent:

```
yc resource-manager folder add-access-binding \
  --id b1g6v0u6dj5boh3nd6vl \
  --role dataproc.agent \
  --service-account-name s3-admin-sa
```

#### 3. Terraform-скрипт

Инфраструктура описана в файле dataproc.tf.

**Структура:**

```
# --- ВОССТАНАВЛИВАЕМ ОПИСАНИЕ СЕТИ ---
# 1. Сервисный аккаунт
data "yandex_iam_service_account" "dataproc_sa" {
  name = "s3-admin-sa"
}

# 2. Сеть
resource "yandex_vpc_network" "dataproc_network" {
  name = "dataproc-network"
}

# 3. NAT-шлюз
resource "yandex_vpc_gateway" "dataproc_nat" {
  name = "dataproc-nat"
  shared_egress_gateway {}
}

# 4. Таблица маршрутизации через NAT
resource "yandex_vpc_route_table" "dataproc_route_table" {
  name       = "dataproc-route-table"
  network_id = yandex_vpc_network.dataproc_network.id
  
  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.dataproc_nat.id
  }
}

# 5. Подсеть
resource "yandex_vpc_subnet" "dataproc_subnet" {
  name           = "dataproc-subnet-a"
  zone           = "ru-central1-a"
  network_id     = yandex_vpc_network.dataproc_network.id
  v4_cidr_blocks = ["192.168.1.0/24"]
  route_table_id = yandex_vpc_route_table.dataproc_route_table.id
}

# 6. Группа безопасности
resource "yandex_vpc_security_group" "dataproc_sg" {
  name       = "dataproc-sg"
  network_id = yandex_vpc_network.dataproc_network.id

  ingress {
    protocol          = "ANY"
    description       = "Allow any ingress traffic inside the security group"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

  ingress {
    protocol       = "TCP"
    description    = "Allow SSH from internet"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 22
    to_port        = 22
  }

  egress {
    protocol          = "ANY"
    description       = "Allow any egress traffic inside the security group"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

  egress {
    protocol       = "ANY"
    description    = "Allow any egress traffic to internet"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 0
    to_port        = 65535
  }
}

resource "yandex_dataproc_cluster" "otus-spark-cluster" {
  bucket = "otus-mlops-ilnur-data"
  cluster_config {
    version_id = "2.1"
    hadoop {
      services = [
        "YARN",
        "SPARK",
        "HDFS"
      ]
      properties = {
      }
      ssh_public_keys = [
        "ssh-rsa XXX== ilnur@otus-mlops"
      ]
      oslogin = false
    }
    subcluster_spec {
      name             = "master"
      role             = "MASTERNODE"
      hosts_count      = 1
      subnet_id        = yandex_vpc_subnet.dataproc_subnet.id
      assign_public_ip = true
      resources {
        disk_size          = 40
        disk_type_id       = "network-ssd"
        resource_preset_id = "s3-c2-m8"
      }
    }
    subcluster_spec {
      name             = "data"
      role             = "DATANODE"
      hosts_count      = 3
      subnet_id        = yandex_vpc_subnet.dataproc_subnet.id
      assign_public_ip = true
      resources {
        disk_size          = 128
        disk_type_id       = "network-ssd"
        resource_preset_id = "s3-c4-m16"
      }
    }
  }
  description = "Spark кластер для проекта"
  folder_id   = "b1g6v0u6dj5boh3nd6vl"
  name        = "otus-spark-cluster"
  security_group_ids = [yandex_vpc_security_group.dataproc_sg.id]
  service_account_id = "ajep3gshld87bul00u57"
  ui_proxy           = true
  zone_id            = "ru-central1-a"
}

```

#### 4. Запуск кластера

**Команды для создания:**

```
cd terraform
#terraform init - выполняем в первый раз
terraform plan
terraform apply
```

#### 5. Проверка через CLI:

```
yc dataproc cluster list
```

**Вывод:**

```
+----------------------+--------------------+---------------------+--------+-------------+---------+
|          ID          |        NAME        |     CREATED AT      | HEALTH | ENVIRNOMENT | STATUS  |
+----------------------+--------------------+---------------------+--------+-------------+---------+
| c9q0jec3pt00694pvnkf | otus-spark-cluster | 2026-08-20 16:17:33 | ALIVE  | PRESTABLE   | RUNNING |
+----------------------+--------------------+---------------------+--------+-------------+---------+
```


