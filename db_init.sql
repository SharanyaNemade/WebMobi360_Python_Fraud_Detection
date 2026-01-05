/* =====================================================
   DATABASE INITIALIZATION SCRIPT
   Project: Fraud Detection Analytics System
   ===================================================== */

CREATE DATABASE IF NOT EXISTS fraud_detection_database;
USE fraud_detection_database;

-- =====================================================
-- TABLE: prediction_history
-- Used for analytics + model predictions
-- =====================================================

CREATE TABLE IF NOT EXISTS prediction_history (
    TransactionId VARCHAR(50) PRIMARY KEY,

    Timestamp DATETIME NOT NULL,
    TransactionType VARCHAR(30) NOT NULL,
    Amount DECIMAL(15,2) NOT NULL,

    senderid VARCHAR(50) NOT NULL,
    receiverid VARCHAR(50) NOT NULL,

    oldbalanceOrg DECIMAL(15,2) NOT NULL,
    newbalanceOrig DECIMAL(15,2) NOT NULL,
    oldbalanceDest DECIMAL(15,2),
    newbalanceDest DECIMAL(15,2),

    IsSuccessful TINYINT(1),

    SenderLocation VARCHAR(100),
    ReceiverLocation VARCHAR(100),

    AmountAnomalyFlag TINYINT(1),
    BehaviorAnomalyFlag TINYINT(1),
    TransactionVelocity24h INT,

    PreviousLinkWithReceiver TINYINT(1) NOT NULL,
    LinkHistoryCount INT NOT NULL,
    SenderAvgTransactionAmount FLOAT(15,2) NOT NULL,

    FraudReasons TEXT,
    Prediction VARCHAR(20),
    FraudRiskScore DECIMAL(5,2),

    SourceType VARCHAR(20),
    BatchID VARCHAR(50),
    SourceFile VARCHAR(100)
);

-- =====================================================
-- TABLE: transaction_history
-- Used for user analysis + dashboards
-- =====================================================

CREATE TABLE IF NOT EXISTS transaction_history (
    id INT AUTO_INCREMENT PRIMARY KEY,

    TransactionID VARCHAR(50),
    Timestamp DATETIME,
    TransactionType VARCHAR(50),
    Amount DECIMAL(12,2),

    SenderID VARCHAR(50),
    ReceiverID VARCHAR(50),

    OldBalanceOrigin DECIMAL(14,2),
    NewBalanceOrigin DECIMAL(14,2),
    OldBalanceDestination DECIMAL(14,2),
    NewBalanceDestination DECIMAL(14,2),

    SenderLocation VARCHAR(100),
    ReceiverLocation VARCHAR(100),

    AmountAnomalyFlag TINYINT,
    BehaviorAnomalyFlag TINYINT,
    TransactionVelocity24h INT,

    PreviousLinkWithReceiver TINYINT,
    LinkHistoryCount INT,
    SenderAvgTransactionAmount DECIMAL(12,2),

    IsFraud VARCHAR(5),        -- '1' or '0'
    FraudRiskScore FLOAT,

    SourceType VARCHAR(20),    -- MANUAL / CSV / LIVE
    BatchID VARCHAR(50),
    SourceFile VARCHAR(255),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- END OF SCRIPT
-- =====================================================
