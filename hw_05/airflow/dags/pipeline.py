"""
DAG: data_cleaning_pipeline
Description: Периодическая очистка датасета мошеннических транзакций.
             Обрабатывает по одному файлу за запуск. Если все три файла
             уже обработаны — Spark-кластер не создаётся.
"""

import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator, BranchPythonOperator
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.task.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



# Переменные из Airflow


YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")     # airflow-dags-...

# Бакет с исходными данными
SOURCE_BUCKET = "otus-mlops-ilnur-data"

# Префиксы
S3_DP_LOGS_BUCKET = f"{S3_BUCKET_NAME}/airflow_logs/"
OUTPUT_PREFIX = "output/hw5/"

DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

# ID подключений (создаются вручную в Airflow UI)
YC_S3_CONN_ID = "yc-s3"
YC_SA_CONN_ID = "yc-sa"


# Список исходных файлов (по порядку)
SOURCE_FILES = [
    "2019-08-22.txt",
    "2019-09-21.txt",
    "2019-10-21.txt",
]



# BranchPythonOperator: выбор ветки

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
    print(f"Found {count} datasets: {datasets}")
    
    if count >= len(SOURCE_FILES):
        return 'finish_task'
    
    next_file = SOURCE_FILES[count]
    full_path = f"s3a://{SOURCE_BUCKET}/{next_file}"
    Variable.set("CURRENT_SOURCE_PATH", full_path)
    return 'dp-cluster-create-task'


# Определение DAG
with DAG(
    dag_id="data_cleaning_pipeline",
    start_date=datetime(2026, 9, 13, 0, 0, 0),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=["otus", "data_cleaning"],
) as dag:

    # 1. Ветвление: обрабатывать или нет
    branch = BranchPythonOperator(
        task_id="pick_file",
        python_callable=choose_branch,
    )

    # 2. Ветка "всё обработано" — пустая задача
    finish_task = EmptyOperator(
        task_id="finish_task",
    )

    # 3. Создание Spark-кластера
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-task",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-dp-{uuid.uuid4()}",
        cluster_description="YDP Cluster for OTUS HW5 - Data Cleaning",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_DP_LOGS_BUCKET,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.1",

        # Группа безопасности (обязательно!)
        security_group_ids=[DP_SECURITY_GROUP_ID],

        # Master node
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=40,

        # Data nodes
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=128,
        datanode_count=2,

        # Compute nodes
        computenode_count=0,

        # Software
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONN_ID,
        dag=dag,
    )

    # 4. Запуск PySpark-скрипта очистки
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-pyspark-task",
        main_python_file_uri=f"s3a://{S3_BUCKET_NAME}/scripts/hw5/pyspark_clean.py",
        cluster_id="{{ ti.xcom_pull(task_ids='dp-cluster-create-task', key='cluster_id') }}",
        connection_id=YC_SA_CONN_ID,
        args=[
            "--source-path", f"{Variable.get('CURRENT_SOURCE_PATH')}",
            "--output-dir", f"s3a://{S3_BUCKET_NAME}/{OUTPUT_PREFIX}",
        ],
        dag=dag,
    )

    # 5. Удаление Spark-кластера
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-task",
        cluster_id="{{ ti.xcom_pull(task_ids='dp-cluster-create-task', key='cluster_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    # Порядок выполнения


    # Ветка 1: есть файлы для обработки → Spark
    branch >> create_spark_cluster >> poke_spark_processing >> delete_spark_cluster

    # Ветка 2: всё обработано → пропустить Spark
    branch >> finish_task