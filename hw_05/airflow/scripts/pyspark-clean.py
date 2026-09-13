"""
Script: pyspark_clean.py
Description: PySpark-скрипт для очистки датасета мошеннических транзакций.
             Читает один файл из бакета с исходными данными, очищает
             и сохраняет результат в Parquet в бакет Airflow.
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F


def clean_data(spark, source_path, output_path):
    """
    Читает файл, очищает данные и сохраняет в Parquet.

    Parameters
    ----------
    spark : SparkSession
        Активная Spark-сессия
    source_path : str
        Полный путь к исходному файлу в S3 (s3a://bucket/file.txt)
    output_path : str
        Директория для сохранения Parquet
    """
    print(f" Reading from: {source_path}")

    # 1. Читаем как текст (файлы имеют заголовок с |, данные с ,)
    raw = spark.read.text(source_path)

    # 2. Пропускаем первую строку (заголовок с |)
    data = raw.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])

    # 3. Преобразуем обратно в DataFrame
    df_raw = data.map(lambda row: Row(value=row.value)).toDF()

    # 4. Разбиваем по запятой на 9 колонок
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

    print(f" Rows before cleaning: {df.count()}")

    # 5. Удаляем дубликаты по transaction_id
    df_clean = df.dropDuplicates(["transaction_id"])

    # 6. Удаляем выбросы по tx_amount (IQR)
    quantiles = df_clean.approxQuantile("tx_amount", [0.25, 0.75], 0.05)
    if quantiles and len(quantiles) == 2:
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        df_clean = df_clean.filter(
            (F.col("tx_amount") >= lower_bound) & (F.col("tx_amount") <= upper_bound)
        )

    # 7. Удаляем строки с некорректной датой (24:00:00)
    df_clean = df_clean.filter(~F.col("tx_datetime").contains("24:00:00"))

    print(f" Rows after cleaning: {df_clean.count()}")

    # 8. Формируем имя выходного файла
    source_name = source_path.split("/")[-1].replace(".txt", "")
    final_output = f"{output_path}{source_name}.parquet"

    # 9. Сохраняем в Parquet
    df_clean.write.mode("overwrite").parquet(final_output)
    print(f" Saved to: {final_output}")


def main():
    """Основная функция для запуска PySpark-задачи."""
    parser = ArgumentParser()
    parser.add_argument(
        "--source-path",
        required=True,
        help="Полный путь к исходному файлу (s3a://bucket/file.txt)"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Директория для сохранения Parquet (s3a://bucket/output/hw5/)"
    )
    args = parser.parse_args()

    # Защита: если Jinja-шаблон не отрендерился, в path попадёт "{{"
    if not args.source_path or "{{" in args.source_path:
        print(f" Invalid source path: {args.source_path}. Exiting.")
        return

    # Создаём Spark-сессию
    spark = (
        SparkSession
        .builder
        .appName("clean-fraud-data")
        .getOrCreate()
    )

    try:
        clean_data(spark, args.source_path, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()