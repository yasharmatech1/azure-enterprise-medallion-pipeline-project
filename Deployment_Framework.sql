CREATE CATALOG IF NOT EXISTS az_prod_catalog;
CREATE SCHEMA IF NOT EXISTS az_prod_catalog.fintech_core_bronze;
CREATE SCHEMA IF NOT EXISTS az_prod_catalog.fintech_core_silver;

-- Create secure Volume path pointers for ADLS Gen2 landing mounts
CREATE VOLUME az_prod_catalog.fintech_core_bronze.vlm_raw_landing;