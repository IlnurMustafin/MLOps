# definitions.py
# Домашнее задание №4 — Feast Feature Views

from datetime import timedelta
import pandas as pd

from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
    FeatureService,
)
from feast.types import Float32, Int64


# 1. Сущность (Entity)

driver = Entity(
    name="driver",
    join_keys=["driver_id"],
)

# 2. Источник данных (Source)

driver_stats_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)


# 3. Feature view #1 — транзакционные признаки

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

# 4. Feature view #2 — поведенческие признаки

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

# 5. Feature Service

driver_feature_service = FeatureService(
    name="driver_fraud_features",
    features=[
        driver_transaction_fv,
        driver_behavior_fv,
    ],
)

# 6. on-demand Feature View

from feast import RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64
import pandas as pd

# Данные, которые приходят в момент запроса
request_source = RequestSource(
    name="trip_request",
    schema=[
        Field(name="current_trip_distance", dtype=Float64),
    ],
)

@on_demand_feature_view(
    sources=[
        driver_transaction_fv,   # отсюда берём conv_rate, acc_rate
        driver_behavior_fv,      # отсюда берём avg_daily_trips
        request_source,          # отсюда берём current_trip_distance
    ],
    schema=[
        Field(name="risk_score", dtype=Float64),
        Field(name="trip_anomaly", dtype=Float64),
    ],
)
def driver_risk_metrics(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    
    # 1. Риск на основе коэффициентов
    df["risk_score"] = (inputs["conv_rate"] * 0.6 + inputs["acc_rate"] * 0.4)
    
    # 2. Аномальность поездки
    df["trip_anomaly"] = inputs["current_trip_distance"] / (inputs["avg_daily_trips"] + 1e-5)
    
    return df
