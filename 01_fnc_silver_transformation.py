# Databricks notebook source
from pyspark.sql.functions import col, when, upper, trim, sha2, concat, lit, to_timestamp, current_timestamp

# Strict Industrial naming paths enforced under Unity Catalog Volumes
VOLUME_BRONZE_PATH = "/Volumes/az_prod_catalog/fintech_core_bronze/vlm_raw_landing/enterprise_transactions_raw.csv"
SILVER_TARGET_TABLE = "az_prod_catalog.fintech_core_silver.tbl_fact_financial_transactions"

print(f"[INFO] Initializing Enterprise Silver Layer Job from UC Volume: {VOLUME_BRONZE_PATH}")

# Read raw immutable staging files with FailFast engine
df_raw_bronze = (spark.read
                 .format("csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .option("mode", "FAILFAST")
                 .load(VOLUME_BRONZE_PATH))

# Advanced Production Transformation Pipeline
df_silver_clean = (
    df_raw_bronze
    # 1. Pipeline Gatekeeping: Purge critical system records missing primary indices
    .dropna(subset=["tx_id", "cust_id"])
    
    # 2. Distributed De-duplication across cluster node evaluation
    .dropDuplicates(["tx_id"])
    
    # 3. String Trimming & Formatting Normalization
    .withColumn("merchant_country", upper(trim(col("merchant_country"))))
    .withColumn("tx_type", upper(trim(col("tx_type"))))
    
    # 4. Data Type Cast Modifications
    .withColumn("tx_amount", col("tx_amount").cast("decimal(18,2)"))
    .withColumn("tx_timestamp", to_timestamp(col("tx_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # 5. Advanced Cryptographic Security Compliances: Hash sensitive account values using SHA-256
    .withColumn("account_hash_identifier", sha2(col("account_number").cast("string"), 256))
    
    # 6. Strict Anti-Money Laundering (AML) Threshold Heuristic Flags
    .withColumn("is_aml_trigger", 
                when((col("tx_amount") >= 10000.00) & (col("tx_type") == "TRANSFER") & (col("merchant_country") != "IN"), lit(1))
                .otherwise(lit(0)))
    
    # 7. Add internal DW audit logs and drop raw un-hashed identity PII columns
    .withColumn("dw_processed_timestamp", current_timestamp())
    .drop("account_number")
)

# Output target writes as optimized managed Delta table format
(df_silver_clean.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(SILVER_TARGET_TABLE))

print(f"[SUCCESS] Silver layer updated successfully: {SILVER_TARGET_TABLE}")