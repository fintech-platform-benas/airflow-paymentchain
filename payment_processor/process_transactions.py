from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import sys

# S3 bucket names
INPUT_BUCKET = "s3a://airflow-paymentchain-dev-ben"
OUTPUT_BUCKET = "s3a://airflow-paymentchain-output-ben"

# Paths
TRANSACTIONS_PATH = f"{INPUT_BUCKET}/transactions.csv"
MERCHANTS_PATH = f"{INPUT_BUCKET}/merchants.csv"
EXCHANGE_RATES_PATH = f"{INPUT_BUCKET}/exchange_rates.json"
FRAUD_RULES_PATH = f"{INPUT_BUCKET}/fraud_rules.json"

OUTPUT_PROCESSED = f"{OUTPUT_BUCKET}/processed/transactions"
OUTPUT_REPORTS = f"{OUTPUT_BUCKET}/reports"
OUTPUT_ALERTS = f"{OUTPUT_BUCKET}/alerts"


# Crear SparkSession

# Ruta a los JARs de Hadoop AWS (montados desde docker-compose como volumen)
JARS_PATH = "/opt/airflow/payment_processor/jars/hadoop-aws-3.3.4.jar,/opt/airflow/payment_processor/jars/aws-java-sdk-bundle-1.12.262.jar"

spark = (SparkSession.builder
    .appName("PaymentChain-Processor-S3")
    .config("spark.jars", JARS_PATH)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate())


print("=" * 60)
print("PAYMENTCHAIN TRANSACTION PROCESSOR (S3)")
print("=" * 60)
print(f"Spark Version: {spark.version}")
print(f"Input Bucket: {INPUT_BUCKET}")
print(f"Output Bucket: {OUTPUT_BUCKET}")
print("=" * 60)

try:

    print("\n[1/7] Loading data from S3...")

    print(f"Intentando leer: {INPUT_BUCKET}/exchange_rates.json")
    df_exchange = spark.read.json(f"{INPUT_BUCKET}/exchange_rates.json")
    print(f"Esquema: {df_exchange.schema}")
    print(f"Count: {df_exchange.count()}")
    df_exchange.show(5)
    
    df_rates = spark.read.json(EXCHANGE_RATES_PATH)
    
    df_txn = spark.read.csv(
        TRANSACTIONS_PATH,
        header=True,
        inferSchema=True
    )
    
    df_merchants = spark.read.csv(
        MERCHANTS_PATH,
        header=True,
        inferSchema=True
    )
        
    df_fraud_rules = spark.read.option("multiLine", True).json(FRAUD_RULES_PATH)
    
    print(f"  ✅ Transactions loaded: {df_txn.count()} rows")
    print(f"  ✅ Merchants loaded: {df_merchants.count()} rows")
    print(f"  ✅ Fraud rules loaded")



    print("\n[2/7] Data validation and cleaning...")

    # Filtrar transacciones válidas (amount es número)
    df_valid = df_txn.filter(col("amount").cast(DoubleType()).isNotNull())

    invalid_count = df_txn.count() - df_valid.count()
    print(f"  - Invalid transactions filtered: {invalid_count}")
    print(f"  - Valid transactions: {df_valid.count()}")

    # Asegurar tipo correcto
    df_valid = df_valid.withColumn("amount", col("amount").cast(DoubleType()))

    # Cache (reutilizaremos múltiples veces)
    df_valid.cache()

    print("\n[3/7] Enrichment (joins with merchants and rates)...")

    # Join con merchants (broadcast - tabla pequeña)
    df_enriched = df_valid.join(
        broadcast(df_merchants),
        on="merchant_id",
        how="left"
    )

    print(f"  - Joined with merchants")

    # Join con exchange rates (broadcast)
    df_enriched = df_enriched.join(
        broadcast(df_rates),
        on="currency",
        how="left"
    )

    print(f"  - Joined with exchange rates")
    print(f"  - Enriched transactions: {df_enriched.count()}")

    print("\n[4/7] Applying transformations...")

    # 1. Calcular fee (1.5% base + ajuste por risk_level)
    df_transformed = df_enriched.withColumn(
        "fee_rate",
        when(col("risk_level") == "HIGH", lit(0.025))
        .when(col("risk_level") == "MEDIUM", lit(0.018))
        .otherwise(lit(0.015))
    )

    df_transformed = df_transformed.withColumn(
        "fee",
        round(col("amount") * col("fee_rate"), 2)
    )

    # 2. Convertir a EUR
    df_transformed = df_transformed.withColumn(
        "amount_eur",
        round(col("amount") * coalesce(col("rate"), lit(1.0)), 2)
    )

    df_transformed = df_transformed.withColumn(
        "fee_eur",
        round(col("fee") * coalesce(col("rate"), lit(1.0)), 2)
    )

    # 3. Total
    df_transformed = df_transformed.withColumn(
        "total_eur",
        col("amount_eur") + col("fee_eur")
    )

    # 4. Categorizar por tamaño (en EUR)
    df_transformed = df_transformed.withColumn(
        "size_category",
        when(col("amount_eur") > 2000, "LARGE")
        .when(col("amount_eur") > 500, "MEDIUM")
        .otherwise("SMALL")
    )

    print(f"  - Transformations applied")

    print("\n[5/7] Fraud detection (window functions)...")

    # Window: Particionar por merchant_id, ordenar por timestamp
    window_velocity = Window.partitionBy("merchant_id") \
        .orderBy("timestamp") \
        .rowsBetween(-4, 0)  # Ventana: 5 transacciones (actual + 4 previas)

    # Contar transacciones en ventana
    df_fraud = df_transformed.withColumn(
        "txn_velocity",
        count("*").over(window_velocity)
    )

    # Detectar high velocity (>= threshold del config)
    fraud_rules = df_fraud_rules.collect()[0].asDict()
    high_velocity_threshold = fraud_rules["high_velocity_threshold"]

    df_fraud = df_fraud.withColumn(
        "high_velocity_flag",
        when(col("txn_velocity") >= high_velocity_threshold, True)
        .otherwise(False)
    )

    # Detectar amounts excesivos (por currency)
    max_amounts = fraud_rules["max_amount_per_currency"]

    df_fraud = df_fraud.withColumn(
        "excessive_amount_flag",
        when(col("currency") == "EUR", col("amount") > max_amounts["EUR"])
        .when(col("currency") == "USD", col("amount") > max_amounts["USD"])
        .when(col("currency") == "GBP", col("amount") > max_amounts["GBP"])
        .when(col("currency") == "JPY", col("amount") > max_amounts["JPY"])
        .when(col("currency") == "CHF", col("amount") > max_amounts["CHF"])
        .otherwise(False)
    )

    # Flag fraude final (cualquiera de las 2 condiciones)
    df_fraud = df_fraud.withColumn(
        "fraud_flag",
        col("high_velocity_flag") | col("excessive_amount_flag")
    )

    fraud_count = df_fraud.filter(col("fraud_flag") == True).count()
    print(f"  - Suspicious transactions detected: {fraud_count}")

    print("\n[6/7] Generating reports (aggregations)...")

    # Reporte 1: Por currency
    df_report_currency = df_fraud \
        .filter(col("status") == "COMPLETED") \
        .groupBy("currency") \
        .agg(
            count("transaction_id").alias("num_transactions"),
            sum("amount_eur").alias("total_amount_eur"),
            avg("amount_eur").alias("avg_amount_eur"),
            sum("fee_eur").alias("total_fee_eur"),
            sum(when(col("fraud_flag") == True, 1).otherwise(0)).alias("fraud_count")
        ) \
        .orderBy(col("total_amount_eur").desc())

    print(f"  - Currency report generated")

    # Reporte 2: Por merchant category
    df_report_category = df_fraud \
        .filter(col("status") == "COMPLETED") \
        .groupBy("category") \
        .agg(
            count("transaction_id").alias("num_transactions"),
            sum("amount_eur").alias("total_amount_eur"),
            avg("fee_eur").alias("avg_fee_eur")
        ) \
        .orderBy(col("total_amount_eur").desc())

    print(f"  - Category report generated")

    # Reporte 3: Top 10 merchants
    df_report_merchants = df_fraud \
        .filter(col("status") == "COMPLETED") \
        .groupBy("merchant_id", "name", "category") \
        .agg(
            count("transaction_id").alias("num_transactions"),
            sum("amount_eur").alias("total_amount_eur")
        ) \
        .orderBy(col("total_amount_eur").desc()) \
        .limit(10)

    print(f"  - Top merchants report generated")

    # Reporte 4: Alertas de fraude (JSON)
    df_fraud_alerts = df_fraud.filter(col("fraud_flag") == True)

    print("\n[7/7] Writing outputs to S3...")

    # Processed transactions (Parquet partitioned by currency)
    df_fraud.write.mode("overwrite") \
        .partitionBy("currency") \
        .parquet(OUTPUT_PROCESSED)
    print(f"  ✅ Processed transactions saved to {OUTPUT_PROCESSED}")
    
    # Reports (CSV)
    df_report_currency.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_REPORTS}/currency_summary")
    print(f"  ✅ Currency report saved")
    
    df_report_category.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_REPORTS}/category_summary")
    print(f"  ✅ Category report saved")
    
    df_report_merchants.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_REPORTS}/top_merchants")
    print(f"  ✅ Top merchants report saved")
    
    # Fraud alerts (JSON)
    df_fraud_alerts.coalesce(1).write.mode("overwrite") \
        .json(f"{OUTPUT_ALERTS}/fraud_alerts")
    print(f"  ✅ Fraud alerts saved")
    
    print("\n" + "=" * 60)
    print("✅ PROCESSING COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Outputs available in: {OUTPUT_BUCKET}")
    print("=" * 60)

except Exception as e:
    print("\n" + "=" * 60)
    print("❌ ERROR DURING PROCESSING")
    print("=" * 60)
    print(f"Error: {str(e)}")
    print("=" * 60)
    sys.exit(1)

finally:
    spark.stop()