# Домашнее задание №5
## Периодический запуск процедуры очистки датасета мошеннических финансовых транзакций

**Курс:** OTUS MLOps  
**Студент:** Ильнур Мустафин  
**Дата:** 13 сентября 2026

---

## 1. Запуск Apache Airflow в Yandex Cloud

### 1.1 Цель

Развернуть управляемый кластер Apache Airflow в Yandex Cloud с помощью Terraform для организации периодического запуска процедуры очистки данных.

---

### 1.2 Архитектура решения

Система состоит из следующих компонентов:

**1. Managed Service for Apache Airflow (Yandex Cloud)**
- **Webserver (UI)** — веб-интерфейс для управления DAG'ами и мониторинга задач.
- **Scheduler** — планировщик, который запускает DAG'и по расписанию.
- **Worker** — исполнитель задач внутри DAG'ов.
- **DAG Processor** — компонент, который обрабатывает DAG-файлы и передаёт их планировщику.

**2. Object Storage (S3)**
- Бакет `airflow-dags-...` для хранения DAG-файлов.
- Airflow автоматически синхронизирует DAG'и из этого бакета.

**3. VPC (сеть и подсеть)**
- Сеть `airflow-network` и подсеть `airflow-subnet-a` обеспечивают сетевую связность для компонентов Airflow.

**Схема взаимодействия:**
DAG-файлы → S3-бакет → DAG Processor → Scheduler → Worker → Выполнение задач

---

### 1.3 Используемые инструменты

| Инструмент | Назначение |
|------------|------------|
| **Terraform** | Управление инфраструктурой как кодом |
| **Yandex Cloud Managed Airflow** | Управляемый сервис Airflow |
| **Object Storage (S3)** | Хранение DAG-файлов |
| **VPC** | Сеть и подсеть для Airflow |

---


### 1.4 Terraform-конфигурация

Полный Terraform-скрипт находится в файле [`airflow.tf`](./hw_05/airflow/airflow.tf).


**Ключевые ресурсы:**

```hcl

# 1. Сеть и подсеть
resource "yandex_vpc_network" "airflow_network" { ... }
resource "yandex_vpc_subnet" "airflow_subnet" {
  name           = "airflow-subnet-a"
  zone           = "ru-central1-a"
  network_id     = yandex_vpc_network.airflow_network.id
  v4_cidr_blocks = ["10.10.1.0/24"]
  route_table_id = yandex_vpc_route_table.nat_route_table.id  # <-- привязка NAT
}

# 2. NAT-шлюз и таблица маршрутизации
resource "yandex_vpc_gateway" "nat_gateway" {
  name = "airflow-nat-gateway"
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "nat_route_table" {
  name       = "airflow-nat-route-table"
  network_id = yandex_vpc_network.airflow_network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

# 3. Сервисный аккаунт с ролями
resource "yandex_iam_service_account" "airflow_sa" { ... }
resource "yandex_resourcemanager_folder_iam_member" "airflow_roles" {
  for_each = toset([
    "storage.viewer",
    "managed-airflow.integrationProvider",
    "vpc.user",
    "logging.writer",
    "monitoring.editor",
  ])
  ...
}

# 4. Бакет для DAG'ов
resource "yandex_storage_bucket" "airflow_dags" { ... }

# 5. Кластер Airflow
resource "yandex_airflow_cluster" "airflow" {
  name           = "test-airflow"
  admin_password = "OtusAirflow2026!"
  subnet_ids     = [yandex_vpc_subnet.airflow_subnet.id]
  ...
  
  # DAG Processor (обязательно для Airflow 3.x)
  dag_processor = { ... }
}
```
---

### 1.5 Почему сделана NAT-маршрутизация

NAT-шлюз и таблица маршрутизации были добавлены не сразу, а после ошибки при создании Spark-кластера:

```text
InvalidArgument: NAT should be enabled on the subnet e9bcju0etq74h6l6qtl0
```

**Причина:** 

Yandex Data Proc требует, чтобы узлы кластера имели доступ в интернет для:

1. Скачивания зависимостей Spark и Hadoop
2. Доступа к Object Storage (S3)
3. Обновления пакетов

**Что было сделано:**

1. Создан NAT-шлюз (yandex_vpc_gateway) — обеспечивает выход в интернет для всей подсети.
2. Создана таблица маршрутизации (yandex_vpc_route_table) с маршрутом 0.0.0.0/0 через NAT-шлюз.
3. Таблица привязана к подсети (route_table_id в yandex_vpc_subnet).

**Результат:** после добавления NAT-шлюза Spark-кластер успешно создаётся.

	Важно: NAT-шлюз тарифицируется (~2 ₽/час), поэтому его нужно удалять вместе с Airflow через terraform destroy.
---

### 1.6 Роли сервисного аккаунта

Для работы Airflow сервисному аккаунту `airflow-sa` были выданы следующие роли:

| Роль | Назначение | Где используется |
|------|------------|------------------|
| `storage.viewer` | Чтение DAG-файлов из S3-бакета | Синхронизация DAG'ов из Object Storage |
| `managed-airflow.integrationProvider` | Интеграция с другими сервисами Yandex Cloud | Работа Airflow с облачными ресурсами |
| `vpc.user` | Использование сети и подсети | Сетевая связность компонентов Airflow |
| `logging.writer` | Запись логов в Cloud Logging | Логирование выполнения DAG'ов |
| `monitoring.editor` | Запись метрик в Cloud Monitoring | Мониторинг работы Airflow |


### 1.7 Доступ к веб-интерфейсу

Ссылка на Airflow UI:
https://c-c9qn2op9aukd2dpojr9s.airflow.yandexcloud.net

	Логин: admin

	Пароль: OtusAirflow2026!

### 1.8 Загрузка DAG в Airflow

Любой DAG можно теперь загрузить в S3-бакет в папку **dags/DAG_XXX/** c помощью команды:

```bash
s3cmd put pipeline_XXX.py s3://airflow-dags-ajehnmi8jhfsm7538lot/dags/DAG_XXX/pipeline_XXX.py
```
Структура в бакете:
```
airflow-dags-ajehnmi8jhfsm7538lot/
└── dags/
    └── DAG_XXX/
        └── pipeline_XXX.py
```

## 2. Создание DAG для очистки данных

### 2.1 Цель

Создать DAG, который ежедневно:
1. Проверяет, остались ли необработанные файлы
2. Создаёт Spark-кластер в Yandex Data Proc
3. Запускает PySpark-скрипт очистки через `spark-submit`
4. Удаляет Spark-кластер

---

### 2.2 Архитектура DAG

| № | Задача | Оператор | Что делает | Следующая задача |
|---|--------|----------|------------|------------------|
| 1 | `pick_file` | `BranchPythonOperator` | Считает датасеты в `output/hw5/`, выбирает ветку | `dp-cluster-create-task` (если файлов < 3)<br>`finish_task` (если файлов >= 3) |
| 2a | `dp-cluster-create-task` | `DataprocCreateClusterOperator` | Создаёт Spark-кластер в Data Proc | `dp-cluster-pyspark-task` |
| 2b | `finish_task` | `EmptyOperator` | Завершает DAG (все файлы обработаны) | — |
| 3 | `dp-cluster-pyspark-task` | `DataprocCreatePysparkJobOperator` | Запускает `pyspark_clean.py` через `spark-submit` | `dp-cluster-delete-task` |
| 4 | `dp-cluster-delete-task` | `DataprocDeleteClusterOperator` | Удаляет Spark-кластер | — |

**Логика работы:**
- **Ветка 1** (файлов < 3): `pick_file → dp-cluster-create → dp-cluster-pyspark → dp-cluster-delete`
- **Ветка 2** (файлов >= 3): `pick_file → finish_task` (Spark-кластер **не создаётся**)

---

### 2.3 Создание подключений вручную

В Airflow 3.x **прямой доступ к базе данных (ORM) запрещён**. Попытка создать подключения программно (как в примере из лекции) приводит к ошибке:
RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0


Поэтому подключения были созданы **вручную** в Airflow UI (Admin → Connections):

| Conn ID | Тип | Назначение |
|---------|-----|------------|
| `yc-s3` | AWS (S3) | Доступ к Object Storage |
| `yc-sa` | Yandex Cloud | Управление Data Proc |

**Почему два подключения:**
1. **`yc-s3`** — Airflow использует для **проверки файлов** в S3 (считает Parquet-датасеты в `output/hw5/`). Spark-кластер **не использует** это подключение — он работает со своим сервисным аккаунтом.
2. **`yc-sa`** — Airflow использует для **управления Data Proc**: создание кластера, запуск задачи, удаление кластера.

**Настройка `yc-s3`:**
- Type: `AWS`
- AWS Access Key ID: `<S3_ACCESS_KEY>`
- AWS Secret Access Key: `<S3_SECRET_KEY>`
- Extra: `{"endpoint_url": "https://storage.yandexcloud.net"}`

**Настройка `yc-sa`:**
- Type: `Yandex Cloud`
- Service account auth JSON: содержимое `key.json`

---

### 2.4 BranchPythonOperator и передача имени файла

**Логика:**
1. `pick_file` считает количество **уникальных датасетов** (директорий `.parquet`) в `output/hw5/`.
2. На основе этого выбирает **следующий исходный файл** из списка `SOURCE_FILES`.
3. Записывает полный путь в **Airflow Variable** `CURRENT_SOURCE_PATH`:

```python
def choose_branch(**kwargs):
    hook = S3Hook(aws_conn_id=YC_S3_CONN_ID)
    keys = hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=OUTPUT_PREFIX)
    
    # Считаем уникальные датасеты (директории .parquet)
    datasets = set()
    for key in (keys or []):
        parts = key.split("/")
        if len(parts) >= 3 and parts[2].endswith(".parquet"):
            datasets.add(parts[2])
    
    count = len(datasets)
    
    if count >= len(SOURCE_FILES):
        return 'finish_task'
    
    next_file = SOURCE_FILES[count]
    full_path = f"s3a://{SOURCE_BUCKET}/{next_file}"
    Variable.set("CURRENT_SOURCE_PATH", full_path)
    return 'dp-cluster-create-task'
```

**Почему через Variable, а не XCom:**

	В DataprocCreatePysparkJobOperator поле args не поддерживает Jinja-шаблоны (template_fields не включает args).

Попытка использовать XCom ({{ ti.xcom_pull(...) }}) приводила к ошибке:

```text
Path does not exist: s3a://.../$ ti.xcom_pull(task_ids='pick_file', key='source_file')
```
Jinja-шаблон не рендерился, и в путь попадала строка с шаблоном.
Использование Variable.get('CURRENT_SOURCE_PATH') в f-string работает, потому что значение читается при парсинге DAG'а (Python-код, а не Jinja).

---

### 2.5 Передача cluster_id через XCom

После создания кластера DataprocCreateClusterOperator автоматически сохраняет ID кластера в XCom под ключом cluster_id.

В задачах запуска скрипта и удаления кластера используется:

```python
cluster_id="{{ ti.xcom_pull(task_ids='dp-cluster-create-task', key='cluster_id') }}"
```

Зачем это нужно:

	dp-cluster-pyspark-task — должен знать, на каком именно кластере запускать PySpark-скрипт.
	dp-cluster-delete-task — должен знать, какой именно кластер удалять.
Как это работает:

1. DataprocCreateClusterOperator создаёт кластер и сохраняет его ID в XCom.
2. DataprocCreatePysparkJobOperator и DataprocDeleteClusterOperator читают ID из XCom через Jinja-шаблон {{ ti.xcom_pull(...) }}.
3. Jinja в cluster_id работает, потому что это поле входит в template_fields (в отличие от args).
**Важно:** DataprocDeleteClusterOperator имеет trigger_rule=TriggerRule.ALL_DONE, чтобы кластер удалялся даже если предыдущая задача упала (экономия ресурсов).

---

### 2.6 Код DAG

Полный код DAG находится в GitHub-репозитории:

| Файл | Путь в репозитории | Назначение |
|------|-------------------|------------|
| DAG | [`hw_05/airflow/dags/pipeline.py`](./hw_05/airflow/dags/pipeline.py) | Описание DAG и всех задач |
| PySpark-скрипт | [`hw_05/airflow/scripts/pyspark_clean.py`](./hw_05/airflow/scripts/pyspark_clean.py) | Скрипт очистки данных |
| Terraform | [`hw_05/airflow/airflow.tf`](./hw_05/airflow/airflow.tf) | Конфигурация инфраструктуры Airflow |

---

### 2.7 Размещение DAG в S3

DAG и скрипт размещены в S3-бакете, который Airflow синхронизирует автоматически:

```bash
# Загрузка DAG
s3cmd put data_pipeline.py s3://airflow-dags-ajehnmi8jhfsm7538lot/dags/hw5/data_pipeline.py

# Загрузка скрипта
s3cmd put pyspark_clean.py s3://airflow-dags-ajehnmi8jhfsm7538lot/scripts/hw5/pyspark_clean.py
```
Структура в бакете:

```text
airflow-dags-ajehnmi8jhfsm7538lot/
├── dags/hw5/
│   └── data_pipeline.py            # DAG
├── scripts/hw5/
│   └── pyspark_clean.py            # PySpark-скрипт
└── output/hw5/                      # Результаты очистки
```

---

### 2.8 Используемые сервисные аккаунты

В проекте задействовано несколько сервисных аккаунтов, каждый из которых решает свою задачу:

| Сервисный аккаунт | Где используется | Роли | Назначение |
|-------------------|------------------|------|------------|
| **`terraform-sa`** | Terraform (`airflow.tf`), подключение `yc-sa` | `editor`, `admin`, `dataproc.agent`, `dataproc.admin` | Управление инфраструктурой Airflow и Data Proc (создание/удаление кластеров) |
| **`s3-admin-sa`** | Terraform (ДЗ №2), `s3cmd` | `storage.admin`, `dataproc.agent` | Управление Object Storage (бакеты, файлы) |
| **`airflow-sa`** | Кластер Managed Airflow | `storage.viewer`, `managed-airflow.integrationProvider`, `monitoring.editor`, `vpc.user`, `logging.writer`, `dataproc.agent`, `dataproc.admin` | Работа Airflow: чтение DAG'ов из S3, логирование, мониторинг, удаление кластеров Data Proc |

**Важное замечание:** 

Yandex Cloud Managed Airflow по умолчанию использует **свой сервисный аккаунт** (`airflow-sa`) для операций с Data Proc, если в операторе явно не указан `connection_id`. В логах это видно как `using metadata service as credentials`. Именно поэтому `airflow-sa` были выданы роли `dataproc.agent` и `dataproc.admin` — без них удаление Spark-кластера падало с `Permission denied`.

---

## 3. Загрузка DAG в Airflow и тестирование

### 3.1 Цель

Убедиться, что DAG:
1. Загрузился в Airflow и отображается в графическом интерфейсе
2. Успешно выполняется по расписанию (не менее трёх запусков)

---

### 3.2 Размещение DAG в S3

DAG-файл и PySpark-скрипт были загружены в S3-бакет, который Airflow синхронизирует автоматически:

```bash
# Загрузка DAG
s3cmd put data_pipeline.py s3://airflow-dags-ajehnmi8jhfsm7538lot/dags/hw5/data_pipeline.py

# Загрузка скрипта очистки
s3cmd put pyspark_clean.py s3://airflow-dags-ajehnmi8jhfsm7538lot/scripts/hw5/pyspark_clean.py
```
----

### 3.3 Проверка загрузки DAG в Airflow UI

После загрузки DAG в S3, Airflow автоматически синхронизировал его и отобразил в графическом интерфейсе.

Что видно в Airflow UI:

1. DAG data_cleaning_pipeline появился в списке DAG'ов
2. Структура DAG отображается в виде графа (Graph View)
3. Все задачи связаны в правильной последовательности

Скриншот структуры DAG:

[`Структура DAG-а`](./hw_05/img/dags_structure.png).

На скриншоте видно:

1. Ветвление pick_file → dp-cluster-create-task / finish_task
2. Последовательность задач Spark-пайплайна
3. Связи между задачами

---

### 3.4 Результаты трёх успешных запусков

DAG был запущен по расписанию (каждые 30 минут). Три запуска прошли успешно:

* 1-й запуск → обработан 2019-08-22.txt
* 2-й запуск → обработан 2019-09-21.txt
* 3-й запуск → обработан 2019-10-21.txt

Скриншот успешных запусков:

[`AIRFLOW-UI`](./hw_05/img/dairflow_UI).

На скриншоте видно:

1. Три запуска DAG'а
2. Все задачи в статусе success (зелёные)
3. Время выполнения каждого запуска
---


### 3.5 Результаты очистки в S3

После трёх успешных запусков в бакете появились три Parquet-датасета:

```text
airflow-dags-ajehnmi8jhfsm7538lot/output/hw5/
├── 2019-08-22.parquet/
├── 2019-09-21.parquet/
└── 2019-10-21.parquet/
```
Скриншот Parquet-файлов в бакете:

[`PARQUETS`](./hw_05/img/parquets_in_bucket.png).


На скриншоте видно:

1. Три директории .parquet в `output/hw5/`
2. Исходные данные были очищены и сохранены в формате Parquet

---
