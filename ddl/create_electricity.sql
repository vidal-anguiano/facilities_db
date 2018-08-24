CREATE TABLE IF NOT EXISTS elec_usage (
invoice_id VARCHAR(16) PRIMARY KEY,
statement_number VARCHAR(10),
account_number VARCHAR(10),
bill_month DATE,
acctg_month DATE,
service_period_start DATE,
service_period_stop DATE,
rebill VARCHAR(1),
billed_khw FLOAT,
peak_kw FLOAT NULL,
supply_charges FLOAT,
udc_charges FLOAT,
total_charges FLOAT
);

CREATE TABLE IF NOT EXISTS elec_accounts (
account_number VARCHAR(10) PRIMARY KEY,
activity_code CHAR(4)
);
