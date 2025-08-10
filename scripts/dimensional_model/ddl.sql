-- Step 1: Create the database
CREATE DATABASE IF NOT EXISTS hospital_star;
USE hospital_star ;

-- Step 2: Dimension Tables

-- dim_patients
-- Step 1: Create the base dim_patients table

CREATE TABLE dim_patients (
    patient_key     INT PRIMARY KEY,
    patientid       VARCHAR(50),
    firstname       VARCHAR(100),
    lastname        VARCHAR(100),
    gender          VARCHAR(10),
    dateofbirth     DATE,
    insurance       VARCHAR(100)
    
);
ALTER TABLE dim_patients
ADD COLUMN effective_date DATE,
ADD COLUMN expiry_date DATE,
ADD COLUMN is_current BOOLEAN,
ADD COLUMN version INT;

-- dim_providers
CREATE TABLE dim_providers (
    provider_key    INT PRIMARY KEY,
    providerid      VARCHAR(50),
    providername    VARCHAR(100),
    specialty       VARCHAR(100),
    department      VARCHAR(100),
    npi             VARCHAR(50)
);

-- dim_procedures
CREATE TABLE dim_procedures (
    procedure_key        INT PRIMARY KEY,
    procedurecode        VARCHAR(50),
    proceduredescription VARCHAR(255)
);

-- dim_date
CREATE TABLE dim_date (
    date_key     INT PRIMARY KEY,
    date_value   DATE,
    day          INT,
    month        INT,
    year         INT,
    quarter      INT,
    day_name     VARCHAR(20),
    month_name   VARCHAR(20)
);

-- Step 3: Fact Tables

-- fact_transactions
CREATE TABLE fact_transactions (
    transaction_key   INT PRIMARY KEY,
    transactionid     VARCHAR(50),
    patient_key       INT,
    provider_key      INT,
    procedure_key     INT,
    date_key          INT,
    amount            DECIMAL(10,2),
    payment_status    VARCHAR(50),
    
    FOREIGN KEY (patient_key) REFERENCES dim_patients(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_providers(provider_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedures(procedure_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- fact_claims
CREATE TABLE fact_claims (
    claim_key        INT PRIMARY KEY,
    claimid          VARCHAR(50),
    patient_key      INT,
    provider_key     INT,
    procedure_key    INT,
    date_key         INT,
    claim_amount     DECIMAL(10,2),
    paid_amount      DECIMAL(10,2),
    claim_status     VARCHAR(50),

    FOREIGN KEY (patient_key) REFERENCES dim_patients(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_providers(provider_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedures(procedure_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);
-- ✅ USE the correct database
USE hospital_star;

-- ========================
--  1. Check Orphaned Records in fact_transactions
-- ========================
SELECT 'Orphaned patient_key in fact_transactions' AS issue, ft.*
FROM fact_transactions ft
LEFT JOIN dim_patients dp ON ft.patient_key = dp.patient_key
WHERE dp.patient_key IS NULL;

SELECT 'Orphaned provider_key in fact_transactions' AS issue, ft.*
FROM fact_transactions ft
LEFT JOIN dim_providers dp ON ft.provider_key = dp.provider_key
WHERE dp.provider_key IS NULL;

SELECT 'Orphaned procedure_key in fact_transactions' AS issue, ft.*
FROM fact_transactions ft
LEFT JOIN dim_procedures dp ON ft.procedure_key = dp.procedure_key
WHERE dp.procedure_key IS NULL;

SELECT 'Orphaned date_key in fact_transactions' AS issue, ft.*
FROM fact_transactions ft
LEFT JOIN dim_date dd ON ft.date_key = dd.date_key
WHERE dd.date_key IS NULL;

-- ========================
--  2. Check Orphaned Records in fact_claims
-- ========================
SELECT 'Orphaned patient_key in fact_claims' AS issue, fc.*
FROM fact_claims fc
LEFT JOIN dim_patients dp ON fc.patient_key = dp.patient_key
WHERE dp.patient_key IS NULL;

SELECT 'Orphaned provider_key in fact_claims' AS issue, fc.*
FROM fact_claims fc
LEFT JOIN dim_providers dp ON fc.provider_key = dp.provider_key
WHERE dp.provider_key IS NULL;

SELECT 'Orphaned procedure_key in fact_claims' AS issue, fc.*
FROM fact_claims fc
LEFT JOIN dim_procedures dp ON fc.procedure_key = dp.procedure_key
WHERE dp.procedure_key IS NULL;

SELECT 'Orphaned date_key in fact_claims' AS issue, fc.*
FROM fact_claims fc
LEFT JOIN dim_date dd ON fc.date_key = dd.date_key
WHERE dd.date_key IS NULL;

-- ========================
--  3. Business Rule Checks
-- ========================
--  Negative or zero amounts in transactions
SELECT 'Invalid transaction amount <= 0' AS issue,
       transaction_key, transactionid, amount
FROM fact_transactions
WHERE amount <= 0;

--  Negative or zero amounts in claims
SELECT 'Invalid claim_amount or paid_amount in claims' AS issue,
       claim_key, claimid, claim_amount, paid_amount
FROM fact_claims
WHERE claim_amount <= 0 OR paid_amount < 0;

--  Invalid or future dates in dim_date
SELECT 'Invalid future or null date_value in dim_date' AS issue,
       date_key, date_value
FROM dim_date
WHERE date_value IS NULL OR date_value > CURDATE();

--  NULL foreign keys in fact_claims
SELECT 'NULL foreign keys in fact_claims' AS issue,
       claim_key, claimid, patient_key, provider_key, procedure_key, date_key
FROM fact_claims
WHERE patient_key IS NULL 
   OR provider_key IS NULL 
   OR procedure_key IS NULL 
   OR date_key IS NULL;

--  NULL foreign keys in fact_transactions
SELECT 'NULL foreign keys in fact_transactions' AS issue,
       transaction_key, transactionid, patient_key, provider_key, procedure_key, date_key
FROM fact_transactions
WHERE patient_key IS NULL 
   OR provider_key IS NULL 
   OR procedure_key IS NULL 
   OR date_key IS NULL;
