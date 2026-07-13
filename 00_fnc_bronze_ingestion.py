# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name

# Enterprise Unity Catalog Paths
VOLUME_RAW_PATH = "/Volumes/az_prod_catalog/fintech_core_bronze/vlm_raw_landing/enterprise_transactions_raw.csv"
BRONZE_TARGET_TABLE = "az_prod_catalog.fintech_core_bronze.tbl_raw_transactions"

print(f"[INFO] Ingesting Raw Assets from Unity Catalog Volume: {VOLUME_RAW_PATH}")

try:
    # Read raw landing data with strict FAILFAST to handle corrupt schemas
    df_raw = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("mode", "FAILFAST")
              .load(VOLUME_RAW_PATH))
    
    # Append crucial data lineage metadata columns
    df_bronze_audit = (df_raw
                       .withColumn("src_file_path", input_file_name())
                       .withColumn("dw_inserted_at", current_timestamp()))
    
    print(f"[INFO] Writing Immutable Snapshot into Delta Layer: {BRONZE_TARGET_TABLE}")
    
    # Save as optimized delta format table governed by Unity Catalog
    (df_bronze_audit.write
     .format("delta")
     .mode("append") # Append style ensures historical snapshots are preserved
     .saveAsTable(BRONZE_TARGET_TABLE))
    
    print("[SUCCESS] Bronze Ingestion Pipeline Execution Complete.")

except Exception as e:
    print(f"[CRITICAL ERROR] Bronze Ingestion Failed: {str(e)}")
    raise e