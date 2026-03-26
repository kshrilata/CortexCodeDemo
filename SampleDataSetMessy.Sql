-- Create a fresh sandbox
CREATE DATABASE IF NOT EXISTS COCO_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS COCO_DEMO_DB.SALES_DATA;
USE SCHEMA COCO_DEMO_DB.SALES_DATA;

-- 1. Table with potential PK (ID) but not defined
CREATE OR REPLACE TABLE CUSTOMERS (
    CUST_ID NUMBER,
    NAME TEXT,
    EMAIL TEXT,
    SIGNUP_DATE DATE
);

INSERT INTO CUSTOMERS VALUES 
(101, 'Alpha Corp', 'contact@alpha.com', '2023-01-01'),
(102, 'Bravo Ltd', 'info@bravo.io', '2023-02-15'),
(103, 'Charlie LLC', 'hello@charlie.com', '2023-03-10');

-- 2. Child table referencing CUST_ID (80%+ match for 'Confirmed')
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID NUMBER,
    CUST_ID NUMBER, -- This relates to CUSTOMERS.CUST_ID
    AMOUNT FLOAT,
    ORDER_STATUS TEXT
);

INSERT INTO ORDERS VALUES 
(1, 101, 500.00, 'SHIPPED'),
(2, 101, 150.00, 'PENDING'),
(3, 102, 1200.00, 'COMPLETED'),
(4, 999, 50.00, 'ERROR'); -- 999 doesn't exist (Testing <100% match)

-- 3. Another table referencing ORDER_ID (Probable match 40-79%)
CREATE OR REPLACE TABLE SHIPMENTS (
    SHIP_ID NUMBER,
    REF_ORDER_ID NUMBER, -- Matches ORDERS.ORDER_ID
    CARRIER TEXT
);

INSERT INTO SHIPMENTS VALUES 
(5001, 1, 'FedEx'),
(5002, 2, 'UPS'),
(5003, 88, 'DHL'); -- 88 doesn't exist (Lowering match percentage)

-- 4. Orphan Table (No relationship)
CREATE OR REPLACE TABLE SETTINGS (
    CONFIG_KEY TEXT,
    CONFIG_VALUE TEXT
);

INSERT INTO SETTINGS VALUES ('THEME', 'DARK'), ('TIMEOUT', '30');
