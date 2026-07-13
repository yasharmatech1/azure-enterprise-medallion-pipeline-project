# 🔷 Azure Enterprise Data Platform: Medallion Architecture Pipeline

[![Azure Data Factory](https://img.shields.io/badge/Orchestration-Azure_Data_Factory_V2-blue?style=for-the-badge&logo=microsoftazure)](https://azure.microsoft.com/)
[![Databricks](https://img.shields.io/badge/Processing-Databricks_Lakehouse-Red?style=for-the-badge&logo=databricks)](https://www.databricks.com/)
[![Azure Synapse](https://img.shields.io/badge/Data_Warehouse-Azure_Synapse_Analytics-DarkBlue?style=for-the-badge&logo=microsoftazure)](https://azure.microsoft.com/)
[![Governance](https://img.shields.io/badge/Governance-Unity_Catalog_Governed-brightgreen?style=for-the-badge)]()

An enterprise-grade, high-volume modern data platform architected entirely on Microsoft Azure. This project implements a fully automated **Medallion Architecture (Bronze -> Silver -> Gold)** running under strict corporate governance constraints via **Unity Catalog**, transforming raw unstructured multi-source financial pipelines into regulatory-compliant compliance dashboards.

---

## 🏗️ 1. Architecture Design & Data Governance Framework

The architecture enforces absolute separation of compute and storage layer elements, transitioning structural components smoothly from ingestion phases directly into curated data warehouse nodes.

```mermaid
graph TD
    %% Source & Ingestion Tier
    subgraph Ingestion_Tier [1. Enterprise Ingestion & Landing]
        A[Multi-Source API/On-Prem SQL] ==>|Azure Data Factory Pipeline| B(ADLS Gen2 Bronze Tier Storage <br> /Volumes/az_prod_catalog/core_bronze/vlm_raw_landing/)
    end

    %% Databricks Computation Core
    subgraph Databricks_Compute [2. Managed Lakehouse Transformation Engine]
        B ==>|Strict PySpark Mode FailFast| C[01_fnc_silver_transformation]
        C -->|SHA-256 PII Cryptography & Deduplication| D[(🥈 Delta Lake Schema <br> az_prod_catalog.fintech_core_silver.tbl_fact_financial_transactions)]
    end

    %% Data Warehouse Materialization
    subgraph Data_Warehouse_Tier [3. Production Enterprise Data Warehouse]
        D ==>|Databricks Synapse PolyBase Connector| E[02_fnc_synapse_gold_load]
        E -->|Materialized Business Matrices Tables| F[(🥇 Azure Synapse Dedicated SQL Pool <br> syn_prod_gold_dw.dbo.tbl_dm_daily_fraud_risk_matrix)]
    end

    %% Control System Layers
    subgraph System_Governance [4. Security, Isolation & Controls]
        G[Databricks Unity Catalog] -.->|Enforces Access Rights ACLs| Databricks_Compute
        H[cluster_policy.json] -.->|Autoscaling Resource Budget Controls| Databricks_Compute
    end

    style B fill:#1e90ff,stroke:#333,stroke-width:2px,color:#fff
    style D fill:#c0c0c0,stroke:#333,stroke-width:2px,color:#000
    style F fill:#ffd700,stroke:#333,stroke-width:2px,color:#000
    style G fill:#32cd32,stroke:#333,stroke-width:2px,color:#fff
