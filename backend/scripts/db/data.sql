ALTER TABLE atm_data ALTER COLUMN location TYPE VARCHAR(50);
ALTER TABLE atm_data ALTER COLUMN atm_bank TYPE VARCHAR(50);

SELECT * FROM atm_data;

ALTER TABLE operational_logs ALTER COLUMN error_code TYPE VARCHAR(50);
ALTER TABLE transactions ALTER COLUMN transaction_status TYPE INT;
ALTER TABLE maintenance_records ALTER COLUMN maintenance_type TYPE VARCHAR(20);