-- Assign ownership and management capabilities to Data Engineering group
GRANT USAGE, CREATE SCHEMA, CREATE TABLE ON CATALOG az_prod_catalog TO `group_data_engineers`;

-- Restrict data read layers over analytical groups
GRANT USAGE ON SCHEMA az_prod_catalog.fintech_core_silver TO `group_data_analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA az_prod_catalog.fintech_core_silver TO `group_data_analysts`;