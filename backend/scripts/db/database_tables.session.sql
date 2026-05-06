
CREATE TABLE atm_data (
    atm_id VARCHAR(10) PRIMARY KEY,
    atm_bank VARCHAR (20),
    location VARCHAR (30),
    atm_model VARCHAR (50)
);

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    atm_id VARCHAR(10) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    withdrawal_amount DECIMAL(10,2),
    transaction_status INT,
    FOREIGN KEY (atm_id) REFERENCES atm_data(atm_id)
);

CREATE TABLE operational_logs (
    log_id SERIAL PRIMARY KEY,
    atm_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    uptime_status INT CHECK (uptime_status IN (0,1)),
    error_code VARCHAR(10),
    downtime_duration INT,
    FOREIGN KEY (atm_id) REFERENCES atm_data(atm_id)
);

CREATE TABLE cash_status (
    cash_id SERIAL PRIMARY KEY,
    atm_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    remaining_cash DECIMAL(12,2),
    FOREIGN KEY (atm_id) REFERENCES atm_data(atm_id)
);

CREATE TABLE maintenance_records (
    maintenance_id SERIAL PRIMARY KEY,
    atm_id VARCHAR(10) NOT NULL,
    maintenance_date TIMESTAMP NOT NULL,
    maintenance_type VARCHAR(50),
    amount_added DECIMAL(12,2),
    FOREIGN KEY (atm_id) REFERENCES atm_data(atm_id)
);


INSERT INTO atm_data (atm_id, atm_bank, location, atm_model)
VALUES ('NCB0001', 'National Commercial Bank', 'NCB UWI Branch', 'NCR_SelfServ_22'),
('NCB0002', 'National Commercial Bank', 'NCB UWI Branch', 'NCR_SelfServ_22'),
('NCB0003', 'National Commercial Bank', 'NCB UWI Branch', 'NCR_SelfServ_22'),
('NCB0004', 'National Commercial Bank', 'NCB UWI Branch', 'DN_Series_100D'),
('NCB0005', 'National Commercial Bank', 'FSS Parking Lot', 'NCR_SelfServ_22'),
('JNB0006', 'Jamaica National Bank', 'JN Bank UWI Branch', 'DN_Series_100D'),
('BNS0007', 'Bank of Nova Scotia', 'Scotiabank UWI Branch', 'NCR_SelfServ_22'),
('BNS0008', 'Bank of Nova Scotia', 'MSBM Parking Lot', 'DN_Series_100D'),
('BNS0009', 'Bank of Nova Scotia', 'MSBM Parking Lot', 'DN_Series_100D'),
('SAG0010', 'Sagicor Bank', 'Student Union', 'NCR_SelfServ_22');