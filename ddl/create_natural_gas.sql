CREATE TABLE IF NOT EXISTS ngas_usage (
account_number VARCHAR(13),
new_account_number VARCHAR(16),
current_account_number VARCHAR(16),
bill_month DATE,
service_period_start DATE,
service_period_stop DATE,
therms FLOAT,
utility_amount FLOAT,
supplier_amount FLOAT,
total_amount FLOAT,
address VARCHAR(60),
address2 VARCHAR(30),
city VARCHAR(15),
PRIMARY KEY (current_account_number, service_period_start, utility_amount, address)
);

CREATE TABLE IF NOT EXISTS ngas_accounts (
account_number VARCHAR(13),
activity_code CHAR(4),
meter_number VARCHAR(9),
type VARCHAR(30),
location VARCHAR(8),
install_date DATE,
ert_meter BOOLEAN,
ert_number VARCHAR(8),
ert_install_date DATE,
PRIMARY KEY (account_number, activity_code)
);
