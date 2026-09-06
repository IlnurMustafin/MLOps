# Домашнее задание №4
## Работа с Feast

**Курс:** OTUS MLOps  
**Студент:** Ильнур Мустафин  
**Дата:** 30 августа 2026

---

## 0. Настройка окружения

### 0.1 Создание виртуального окружения

Для изоляции зависимостей проекта было создано отдельное виртуальное окружение Python 3.12:

```
cd ~/Documents/ML/MLOps_OtusRu/MLOps/hw_04
python3 -m venv feast_env
source feast_env/bin/activate
```
### 0.2 Установка Feast и зависимостей


```
pip install --upgrade pip
pip install feast pandas jupyter
```

### 0.3 Инициализация репозитория Feast
```
feast init feature_repo
cd feature_repo
```

### 0.4 Проверка работоспособности

```
python -c "import feast; print(feast.__version__)"
0.66.0
```

### 0.5 Активация окружения при повторной работе

```
cd ~/Documents/ML/MLOps_OtusRu/MLOps/hw_04
source feast_env/bin/activate
```

## 1. Создание Feature Views

### 1.1 Сущность (Entity)

Для работы с признаками была создана сущность `driver`, представляющая водителя. Сущность идентифицируется по полю `driver_id`:

```python
from feast import Entity

driver = Entity(
    name="driver",
    join_keys=["driver_id"],
)
```

### 1.2 Источник данных

В качестве источника данных используется файл driver_stats.parquet из лекционного примера. Файл содержит исторические данные о поездках водителей:

* driver_id — идентификатор водителя
* event_timestamp — временная метка
* conv_rate — коэффициент конверсии
* acc_rate — коэффициент принятия
* avg_daily_trips — среднее число поездок в день

```python
from feast import FileSource

driver_stats_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```

### 1.3 Feature View #1 — транзакционные признаки

Первая Feature View объединяет транзакционные признаки водителя: коэффициент конверсии и коэффициент принятия. Эти признаки отражают краткосрочное поведение водителя и обновляются ежедневно (TTL = 1 день).

```python
from feast import FeatureView, Field
from feast.types import Float32
from datetime import timedelta

driver_transaction_fv = FeatureView(
    name="driver_transaction_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "fraud_detection", "group": "transactional"},
)
```

### 1.4 Feature View #2 — поведенческие признаки

Вторая Feature View содержит поведенческие признаки водителя: среднее количество поездок в день. Этот признак отражает долгосрочные паттерны поведения и обновляется реже (TTL = 7 дней).
```python
from feast.types import Int64

driver_behavior_fv = FeatureView(
    name="driver_behavior_features",
    entities=[driver],
    ttl=timedelta(days=7),
    schema=[
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "fraud_detection", "group": "behavioral"},
)
```

### 1.5 Feature Service (опционально)

Для удобства использования Feature Views в модели создан Feature Service driver_fraud_features, объединяющий обе Feature View:

```python
from feast import FeatureService

driver_feature_service = FeatureService(
    name="driver_fraud_features",
    features=[
        driver_transaction_fv,
        driver_behavior_fv,
    ],
)
```


### 1.6 Регистрация Feature Views

После описания всех компонентов Feature Views были зарегистрированы в Feast с помощью команды:

```bash
feast apply
```

Результат:

```text
Created project feature_repo
Created entity driver
Created feature view driver_behavior_features
Created feature view driver_transaction_stats
Created feature service driver_fraud_features
```

### 1.7 Проверка созданных Feature Views

```bash
(feast_env) (base) ilnurmustafin@Mac feature_repo % feast feature-views list
NAME                      ENTITIES    TYPE         ENABLED    STATE
driver_behavior_features  {'driver'}  FeatureView  Yes        STATE_UNSPECIFIED
driver_transaction_stats  {'driver'}  FeatureView  Yes        STATE_UNSPECIFIED
```
## 2. On-Demand Feature View

### 2.1 Цель

Создать On-Demand Feature View для вычисления признаков в реальном времени на основе существующих данных.

### 2.2 Назначение

On-Demand Feature View `driver_risk_metrics` вычисляет признаки риска для водителя на основе:
- Исторических коэффициентов `conv_rate` и `acc_rate`
- Параметров текущего запроса (`current_trip_distance`)

### 2.3 Код

```python
from feast import Field, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64
import pandas as pd

# Данные, поступающие с запросом
request_source = RequestSource(
    name="trip_request",
    schema=[
        Field(name="current_trip_distance", dtype=Float64),
    ],
)

@on_demand_feature_view(
    sources=[driver_transaction_fv, request_source],
    schema=[
        Field(name="risk_score", dtype=Float64),
        Field(name="trip_anomaly", dtype=Float64),
    ],
)
def driver_risk_metrics(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["risk_score"] = (inputs["conv_rate"] * 0.6 + inputs["acc_rate"] * 0.4)
    df["trip_anomaly"] = inputs["current_trip_distance"] / (inputs["avg_daily_trips"] + 1e-5)
    return df
```

### 2.4 Регистрация

```bash
feast apply
```

Результат:

```text
Created on demand feature view driver_risk_metrics
```

### 2.5 Проверка созданных Feature Views

```bash
(feast_env) (base) ilnurmustafin@Mac feature_repo % feast feature-views list
NAME                      ENTITIES    TYPE                 ENABLED    STATE
driver_behavior_features  {'driver'}  FeatureView          Yes        STATE_UNSPECIFIED
driver_transaction_stats  {'driver'}  FeatureView          Yes        STATE_UNSPECIFIED
driver_risk_metrics       {'driver'}  OnDemandFeatureView  Yes        STATE_UNSPECIFIED
```
## 3. Запросы к Feature Views (ноутбук)

### 3.1 Цель

Создать ноутбук с примерами получения признаков из Feature Views для:
- **Исторических данных** — для обучения модели
- **Онлайн-данных** — для инференса в реальном времени

---

### 3.2 Ноутбук с запросами

Для демонстрации работы с Feature Views создан ноутбук `query_notebook.ipynb`.

**Структура ноутбука:**

| Раздел | Описание |
|--------|----------|
| 1. Инициализация | Подключение к Feature Store |
| 2. Historical Features | Получение признаков для обучения модели (Offline Store) |
| 3. Online Features | Получение признаков для инференса (Online Store) 

---

### 3.3 Исторические признаки (для обучения)

**Запрос:**

```python
entity_df = pd.DataFrame.from_dict({
    "driver_id": [1001, 1002, 1003],
    "event_timestamp": [
        datetime(2021, 4, 12, 10, 59, 42),
        datetime(2021, 4, 12, 8, 12, 10),
        datetime(2021, 4, 12, 16, 40, 26),
    ],
    "current_trip_distance": [15.5, 8.2, 42.1],
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=store.get_feature_service("driver_fraud_features"),
).to_df()
```

### 3.4 Онлайн-признаки (для инференса)

**Запрос через Feature Service:**

```python
online_features = store.get_online_features(
    features=store.get_feature_service("driver_fraud_features"),
    entity_rows=[
        {"driver_id": 1001},
        {"driver_id": 1002},
        {"driver_id": 1003},
    ],
).to_dict()
```

### 3.5 Онлайн-признаки с On-Demand трансформациями

**Запрос с On-Demand Feature View:**

```python
online_features_with_risk = store.get_online_features(
    features=[
        "driver_transaction_stats:conv_rate",
        "driver_transaction_stats:acc_rate",
        "driver_behavior_features:avg_daily_trips",
        "driver_risk_metrics:risk_score",
        "driver_risk_metrics:trip_anomaly",
    ],
    entity_rows=[
        {"driver_id": 1001, "current_trip_distance": 15.5},
        {"driver_id": 1002, "current_trip_distance": 8.2},
        {"driver_id": 1003, "current_trip_distance": 42.1},
    ],
).to_dict()
```

### 3.6 Материализация признаков в онлайн-хранилище

```python
# 3.1 Сначала материализуем признаки в онлайн-хранилище
from datetime import datetime
from feast import FeatureStore
# Инициализация
store = FeatureStore(repo_path=".")

# Материализуем все данные
store.materialize_incremental(end_date=datetime.now())
```

### 3.7 Проверка онлайн-хранилища

```bash
sqlite3 data/online_store.db ".tables"
```
