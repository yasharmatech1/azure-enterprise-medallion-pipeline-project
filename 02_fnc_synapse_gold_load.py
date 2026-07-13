# Databricks notebook source
from pyspark.sql.functions import sum, count, round, col

SILVER_SOURCE = "az_prod_catalog.fintech_core_silver.tbl_fact_financial_transactions"
SYNAPSE_TARGET_TABLE = "syn_prod_gold_dw.dbo.tbl_dm_daily_fraud_risk_matrix"

print("[INFO] Initiating Gold Analytical Aggregation Phase...")

df_silver_fact = spark.read.table(SILVER_SOURCE)

# Constructing high-value business aggregates views
df_gold_kpis = (
    df_silver_fact
    .groupBy("merchant_country", "tx_type")
    .agg(
        count("tx_id").alias("total_transaction_volume"),
        round(sum("tx_amount"), 2).alias("net_settlement_currency_amount"),
        sum("is_aml_trigger").alias("total_flagged_compliance_violations")
    )
)

print(f"[INFO] Pushing aggregated matrices to Azure Synapse Data Warehouse target table: {SYNAPSE_TARGET_TABLE}")

# Establish secure high-speed optimized PolyBase configuration mapping parameters
# In enterprise settings, these credentials are contextually masked inside Azure Key Vault
dw_jdbc_url = "jdbc:sqlserver://syn-prod-workspace.sql.azuresynapse.net:1433;database=syn_prod_gold_dw"
temp_adls_storage_dir = "abfss://temp-container@adlsgen2storage.dfs.core.windows.net/synapse_polybase_temp"

(df_gold_kpis.write
 .format("com.databricks.spark.synapse")
 .option("url", dw_jdbc_url)
 .option("dbtable", SYNAPSE_TARGET_TABLE)
 .option("forwardSparkAzureStorageCredentials", "true")
 .option("tempDir", temp_adls_storage_dir)
 .mode("overwrite")
 .save())

print("[SUCCESS] Production Warehouse Gold Datamart compilation complete.")