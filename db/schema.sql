CREATE TABLE IF NOT EXISTS transactions (
    event_id VARCHAR(255) PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    amount DECIMAL(19, 4) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    from_account VARCHAR(255),
    to_account VARCHAR(255),
    transaction_type VARCHAR(50),
    device_id VARCHAR(255),
    ip_address VARCHAR(45),
    location TEXT,
    authentication_method VARCHAR(50),
    is_international BOOLEAN,
    source_platform VARCHAR(50),
    fraud_status VARCHAR(20) NOT NULL,
    fraud_detection_reason TEXT,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);