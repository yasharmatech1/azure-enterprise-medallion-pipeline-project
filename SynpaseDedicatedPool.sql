CREATE DATABASE syn_prod_gold_dw;
GO
CREATE TABLE syn_prod_gold_dw.dbo.tbl_dm_daily_fraud_risk_matrix (
    merchant_country VARCHAR(10),
    tx_type VARCHAR(50),
    total_transaction_volume BIGINT,
    net_settlement_currency_amount DECIMAL(18,2),
    total_flagged_compliance_violations INT
);