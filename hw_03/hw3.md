# Домашнее задание №3
## Анализ качества и очистка датасета мошеннических финансовых операций

**Курс:** OTUS MLOps  
**Студент:** Ильнур Мустафин  
**Дата:** 26 августа 2026

---

## 1. Сервисный аккаунт для работы с кластером Data Processing

### Текущее состояние

Для работы с кластером Yandex Data Processing используется ранее созданный сервисный аккаунт `s3-admin-sa`.

### Создание сервисного аккаунта

Аккаунт был создан в рамках предыдущего домашнего задания:

```
yc iam service-account create --name s3-admin-sa
```

### Назначенные роли

Для работы с кластером и объектным хранилищем сервисному аккаунту были выданы следующие роли:

* storage.admin: полный доступ к Object Storage (создание бакетов, запись/чтение данных)
* dataproc.agent: управление кластером Data Processing

### Команды для назначения ролей
```
# Роль storage.admin
yc resource-manager folder add-access-binding \
  --id b1g6v0u6dj5boh3nd6vl \
  --role storage.admin \
  --service-account-name s3-admin-sa

# Роль dataproc.agent
yc resource-manager folder add-access-binding \
  --id b1g6v0u6dj5boh3nd6vl \
  --role dataproc.agent \
  --service-account-name s3-admin-sa
```
> **Примечание:** Роль `editor` также присутствует у сервисного аккаунта, но она была назначена по умолчанию при его создании и не является ключевой для выполнения задач ДЗ.
### Проверка прав

```
yc resource-manager folder list-access-bindings b1g6v0u6dj5boh3nd6vl
```

В списке присутствуют записи для s3-admin-sa с ролями storage.admin и dataproc.agent.

```
+----------------+----------------+----------------------+
|    ROLE ID     |  SUBJECT TYPE  |      SUBJECT ID      |
+----------------+----------------+----------------------+
| editor         | serviceAccount | ajefflvjfg3ogakm0n41 |
| storage.admin  | serviceAccount | ajep3gshld87bul00u57 |
| dataproc.agent | serviceAccount | ajep3gshld87bul00u57 |
+----------------+----------------+----------------------+
```

В списке присутствуют записи для s3-admin-sa с ролями storage.admin и dataproc.agent.

## 2. Бакет в Object Storage

### Текущее состояние

Для хранения данных используется бакет otus-mlops-ilnur-data, созданный в рамках предыдущего домашнего задания. Бакет не был удалён после завершения ДЗ №2 и продолжает хранить датасеты для анализа.

### Создание бакета

Бакет был создан с помощью Terraform:
```
hcl
resource "yandex_storage_bucket" "my_bucket" {
  bucket   = "otus-mlops-ilnur-data"
  acl      = "private"
  max_size = 107374182400
}
```
### Права доступа

Бакет настроен следующим образом:

* Запись: разрешена для сервисного аккаунта s3-admin-sa (роль storage.admin)
* Чтение: публичный доступ открыт для проверки преподавателем

Публичный доступ был обеспечен при копировании данных с помощью флага --acl-public:

```
s3cmd cp --acl-public --recursive s3://otus-mlops-source-data/ s3://otus-mlops-ilnur-data/
```

Флаг --acl-public автоматически установил публичные права на чтение для всех скопированных файлов.

### Содержимое бакета

В бакете находятся 5 файлов с данными о транзакциях:

```
2019-08-22.txt  (2.8 ГБ)
2019-09-21.txt  (2.85 ГБ)
2019-10-21.txt  (2.89 ГБ)
2019-11-20.txt  (2.94 ГБ)
2019-12-20.txt  (2.99 ГБ)
```

### Публичная ссылка для проверки

Публичная ссылка на бакет:
```
https://storage.yandexcloud.net/otus-mlops-ilnur-data/
```
Пример файла:
```
https://storage.yandexcloud.net/otus-mlops-ilnur-data/2019-08-22.txt
```

## 3. Создание Spark-кластера с доступом к Jupyter Notebook

### Цель

Создать Spark-кластер в Yandex Data Processing с двумя подкластерами (мастер и 3 data-узла) и настроить доступ к Jupyter Notebook для анализа данных.

---

### Конфигурация кластера

Кластер создан с использованием Terraform (скрипт из ДЗ №2) и имеет следующие характеристики:

| Параметр | Master-подкластер | Data-подкластер |
|----------|-------------------|-----------------|
| **Класс хоста** | `s3-c2-m8` | `s3-c4-m16` |
| **Количество хостов** | 1 | 3 |
| **Размер хранилища** | 40 ГБ SSD | 128 ГБ SSD |
| **Роль** | MASTERNODE | DATANODE |
| **Публичный IP** | Включён | Включён |

**Версия образа:** `2.1`  
**Компоненты:** `HDFS`, `YARN`, `SPARK`

---

### Настройка UI Proxy

Для безопасного доступа к веб-интерфейсам кластера (Jupyter, Spark History Server, YARN Resource Manager) был включён **UI Proxy**. В Terraform-конфигурации был установлен параметр:

```
ui_proxy = true
```

### Получение ссылок на веб-интерфейсы:

```
yc dataproc cluster list-ui-links otus-spark-cluster
```
| Веб-интерфейс | URL |
|---------------|-----|
| Spark History Server Web UI | `https://ui-...dataproc-ui.yandexcloud.net/` |
| YARN Resource Manager Web UI | `https://ui-...dataproc-ui.yandexcloud.net/` |
| JobHistory Server Web UI | `https://ui-...dataproc-ui.yandexcloud.net/` |
| HDFS Namenode UI | `https://ui-...dataproc-ui.yandexcloud.net/` |


> **Примечание:** Jupyter Notebook не входит в стандартный образ 2.1, поэтому он был установлен вручную на мастер-узле.

### Установка Jupyter Notebook

#### Для выполнения анализа данных на мастер-узле был установлен Jupyter Notebook:

**1. Подключение по SSH к мастер-узлу**

```
ssh -i ~/.ssh/id_rsa ubuntu@<публичный_IP_мастера>
```

**2. Установка Jupyter**

```
sudo apt update
sudo apt install python3-pip -y
pip3 install notebook
```

**3. Запуск Jupyter Notebook**

```
python3 -m notebook --ip=0.0.0.0 --port=8888 --no-browser
```

После запуска был получен токен для доступа:

```
http://127.0.0.1:8888/tree?token=12bb4XXX
```

**4. Настройка группы безопасности (через веб-интерфейс)**

Для доступа к Jupyter Notebook из интернета в группе безопасности `dataproc-sg` было добавлено правило через веб-консоль Yandex Cloud.

**Действия в консоли:**

1. Перейти в раздел **Virtual Private Cloud** → **Группы безопасности**.
2. Выбрать группу `dataproc-sg`.
3. Нажать кнопку **"Добавить правило"**.
4. Заполнить поля для входящего трафика:

| Поле | Значение |
|------|----------|
| **Направление** | Входящий трафик |
| **Протокол** | `TCP` |
| **Диапазон портов** | `8888` |
| **Тип источника** | `CIDR` |
| **Значение источника** | `0.0.0.0/0` |
| **Описание** | `Allow Jupyter Notebook from internet` |

5. Нажать **"Сохранить"**.

После добавления правила Jupyter Notebook стал доступен по адресу:

```
http://<публичный_IP_мастера>:8888/tree?token=<токен>
```

> **Примечание:** Правило открывает доступ к порту 8888 для всех IP-адресов (`0.0.0.0/0`), что допустимо в учебных целях. В реальных проектах рекомендуется ограничивать доступ конкретными IP-адресами.

#### Запуск Spark в Jupyter

Для работы с Spark в Jupyter были добавлены пути к библиотекам:
```
python
import os
import sys

# Указываем путь к Spark
os.environ['SPARK_HOME'] = '/usr/lib/spark'

# Добавляем пути к pyspark и py4j
sys.path.append(os.path.join('/usr/lib/spark', 'python'))
sys.path.append(os.path.join('/usr/lib/spark', 'python/lib/py4j-0.10.9.5-src.zip'))

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FraudAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("Spark version:", spark.version)
```

Результат:

```
Spark version: 3.3.2
```

Spark успешно запущен и готов к анализу данных.

## 4. Анализ качества данных

Для анализа был выбран один файл `2019-08-22.txt` (46.9 млн записей, ~2.8 ГБ) с целью экономии ресурсов на Облаке.

### Результаты анализа

| Метрика | Значение |
|---------|----------|
| Всего записей | 46 988 418 |
| Дубликаты | 181 (0.0004%) |
| NULL-значения | 0 во всех колонках |
| Нарушения бизнес-правил | 0 |
| Выбросы (IQR, tx_amount) | 1 309 077 (2.79%) |
| Средняя сумма транзакции | 54.23 |
| Максимальная сумма | 3773.34 |

### Выводы

1. **NULL-значения отсутствуют** — проверка прошла успешно.
2. **Дубликаты есть, но их мало** — их можно удалить.
3. **Бизнес-правила соблюдены** — `tx_fraud` и `tx_fraud_scenario` согласованы.
4. **Выбросы составляют ~2.79%** — их следует обработать (удалить или заменить) перед обучением модели.
5. **Сумма транзакций варьируется от 0 до 3773.34** — значения выглядят реалистично.

### Код чтения данных

```
# Читаем только один файл
file_name = "2019-08-22.txt"
path = f"s3a://otus-mlops-ilnur-data/{file_name}"

print(f"Reading: {file_name}")

# 1. Читаем как текст
raw = spark.read.text(path)

# 2. Пропускаем первую строку (заголовок с |)
data = raw.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])

# 3. Преобразуем обратно в DataFrame
from pyspark.sql import Row
df_raw = data.map(lambda row: Row(value=row.value)).toDF()

# 4. Разбиваем по запятой
from pyspark.sql import functions as F

df = df_raw.select(
    F.split(F.col("value"), ",").alias("cols")
).select(
    F.col("cols")[0].cast("int").alias("transaction_id"),
    F.col("cols")[1].alias("tx_datetime"),
    F.col("cols")[2].cast("int").alias("customer_id"),
    F.col("cols")[3].cast("int").alias("terminal_id"),
    F.col("cols")[4].cast("double").alias("tx_amount"),
    F.col("cols")[5].cast("int").alias("tx_time_seconds"),
    F.col("cols")[6].cast("int").alias("tx_time_days"),
    F.col("cols")[7].cast("int").alias("tx_fraud"),
    F.col("cols")[8].cast("int").alias("tx_fraud_scenario")
)

print(f"Rows: {df.count()}")
df.show(5, truncate=False)
```

### Функция анализа данных

```
from pyspark.sql.functions import col, isnull, count, when

def analyze_file(df, file_name):
    """
    Анализирует DataFrame и выводит статистику
    """
    print(f"ANALYSIS: {file_name}")

    # 1. Общая информация
    print("\n1. SCHEMA:")
    df.printSchema()

    # 2. Количество записей
    total = df.count()
    print(f"\n2. Total rows: {total}")

    # 3. Пример данных
    print("\n3. Sample data:")
    df.show(5, truncate=False)

    # 4. Проверка на нуллы (исключая transaction_id)
    null_columns = [c for c in df.columns if c != "transaction_id"]
    null_counts = df.select([
        count(when(isnull(col(c)), c)).alias(c) for c in null_columns
    ]).collect()[0]

    print("\n4. Null values (excluding transaction_id):")
    for col_name, null_count in zip(null_columns, null_counts):
        print(f"   {col_name}: {null_count}")

    # 5. Дубликаты
    unique = df.select("transaction_id").distinct().count()
    duplicates = total - unique
    print(f"\n5. Duplicates: {duplicates}")

    # 6. Бизнес-правила
    inconsistent1 = df.filter((col("tx_fraud") == 0) & (col("tx_fraud_scenario") != 0)).count()
    inconsistent2 = df.filter((col("tx_fraud") == 1) & (col("tx_fraud_scenario") == 0)).count()
    print(f"\n6. Business rules violations:")
    print(f"   tx_fraud=0 but scenario!=0: {inconsistent1}")
    print(f"   tx_fraud=1 but scenario=0: {inconsistent2}")

    # 7. Статистика по сумме транзакций
    print(f"\n7. tx_amount statistics:")
    df.select("tx_amount").describe().show()

    # 8. Выбросы через межквартильный размах (IQR)
    print("\n8. Outliers detection (IQR method):")

    outliers_count = 0
    quantiles = df.approxQuantile("tx_amount", [0.25, 0.75], 0.05)
    if quantiles and len(quantiles) == 2:
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers_count = df.filter(
            (col("tx_amount") < lower_bound) | (col("tx_amount") > upper_bound)
        ).count()
        outliers_percent = (outliers_count / total) * 100

        print(f"   Q1: {q1:.2f}")
        print(f"   Q3: {q3:.2f}")
        print(f"   IQR: {iqr:.2f}")
        print(f"   Lower bound: {lower_bound:.2f}")
        print(f"   Upper bound: {upper_bound:.2f}")
        print(f"   Outliers (IQR): {outliers_count} ({outliers_percent:.2f}%)")
    else:
        print("   Not enough data to compute quantiles.")

    return {
        "total": total,
        "duplicates": duplicates,
        "inconsistent1": inconsistent1,
        "inconsistent2": inconsistent2,
        "outliers": outliers_count
    }
```


### Результаты анализа данных

```
stats = analyze_file(df, file_name)


print(f"SUMMARY FOR {file_name}")

for key, value in stats.items():
    print(f"{key}: {value}")
```

> Было найдено два типа некорретных данных: дубликаты и выбросы.

Так же проведена проверка на корректность дат:

```
from pyspark.sql.functions import to_timestamp, col

# Проверяем, что даты парсятся корректно
invalid_date = df.filter(to_timestamp(col("tx_datetime"), "yyyy-MM-dd HH:mm:ss").isNull())
print(f"Invalid date format: {invalid_date.count()}")
```

Найдены объекты в датафрейме даты со временем ```24:00:00``` - что является третьим типом некорретных данных.

## 5 - 6. Скрипт очистки данных. Сохранение очищенных данных в Parquet

### Цель

На основе проведённого анализа разработан скрипт очистки данных на Apache Spark. Скрипт автоматически удаляет дубликаты, выбросы и некорректные значения. Датасет загружен в бакет в формате **Parquet**.

---

### Функция очистки

```
from pyspark.sql.functions import col

def clean_data(df):
    """
    Очищает DataFrame от выбросов и дубликатов
    """
    # 1. Удаляем дубликаты по transaction_id
    df = df.dropDuplicates(["transaction_id"])
    
    # 2. Удаляем выбросы по tx_amount (метод IQR)
    quantiles = df.approxQuantile("tx_amount", [0.25, 0.75], 0.05)
    if quantiles and len(quantiles) == 2:
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        df = df.filter(
            (col("tx_amount") >= lower_bound) & (col("tx_amount") <= upper_bound)
        )
    
    # 3. Удаляем строки с отрицательной суммой (если есть)
    df = df.filter(col("tx_amount") >= 0)
    
    return df
```

### Применение скрипта
```
print(f"CLEANING DATA FOR {file_name}")

df_clean = clean_data(df)

# Удаляем записи с 24:00:00
df_clean_fin = df_clean.filter(~col("tx_datetime").contains("24:00:00"))

print(f"Rows after cleaning: {df_clean_fin.count()}")
```

### Результат очистки

| Метрика | До очистки | После очистки | Изменение |
|---------|------------|---------------|-----------|
| Всего записей | 46 988 418 | 45 670 214 | −1 318 204 |
| Дубликаты | 181 | 0 | −181 |
| Выбросы (IQR) | 1 306 492 | 0 | −1 306 492 |
| Некорректный формат даты | 100 | 0 | −100 |

**Итоговый объём очищенных данных:** **45 670 214** записей (снижение на **2.80%**).

---

### Что делает скрипт

| Шаг | Действие | Обоснование |
|-----|----------|-------------|
| 1 | Удаление дубликатов по `transaction_id` | Устранение повторяющихся транзакций |
| 2 | Удаление выбросов по `tx_amount` (IQR) | Устранение аномально высоких/низких сумм |
| 3 | Удаление записей с некорретным форматом даты | ```24:00:00``` - некорректное время |


### Код сохранения

```
# Сохраняем очищенный DataFrame в Parquet
# Параметр сжатия установлен по умолчанию (Snappy).
output_path = f"s3a://otus-mlops-ilnur-data/cleaned_fin/{file_name.replace('.txt', '.parquet')}"

df_clean_fin.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Saved cleaned data to: {output_path}")
```

Файлы сохранены в `s3://otus-mlops-ilnur-data/cleaned_fin/`. Проверить это можно командой `yc storage s3api list-objects-v2 --bucket otus-mlops-ilnur-data --prefix cleaned_fin/` (вывод сохранён в репозитории).

*Для предотвращения лишних расходов кластер был удалён с помощью Terraform:*

```
cd terraform
terraform destroy
```