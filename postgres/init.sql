CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_email VARCHAR(255),
    amount DECIMAL(10,2),
    product VARCHAR(100),
    status VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50),
    check_name VARCHAR(100),
    column_name VARCHAR(50),
    anomaly_value VARCHAR(255),
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomaly_reports (
    report_id SERIAL PRIMARY KEY,
    anomaly_id INTEGER REFERENCES anomalies(anomaly_id),
    llm_summary TEXT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_name VARCHAR(100),
    rows_processed INTEGER,
    status VARCHAR(20),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
