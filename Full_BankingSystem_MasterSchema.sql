
-- ===============================================================
--             BANKING SYSTEM DATABASE SCHEMA
-- ===============================================================

DROP DATABASE IF EXISTS BankingSystem;
CREATE DATABASE BankingSystem;
USE BankingSystem;

-- ======================================
--  DIMENSIONS IN SNOWFLAKE SCHEMA
-- ======================================
CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Phone VARCHAR(20),
    CreatedAt DATETIME DEFAULT GETDATE(),
    IsLocked BIT DEFAULT 0
);

CREATE TABLE DimAddress (
    AddressID INT PRIMARY KEY,
    CustomerID INT,
    Street VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    ZipCode VARCHAR(10),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

CREATE TABLE DimAccountType (
    AccountTypeID INT PRIMARY KEY,
    AccountTypeName VARCHAR(50)
);

-- =========================
--  FACT TABLES
-- =========================
CREATE TABLE FactAccount (
    AccountID INT PRIMARY KEY,
    CustomerID INT,
    AccountTypeID INT,
    Balance DECIMAL(18,2) CHECK (Balance >= 0),
    AccountStatus VARCHAR(20) DEFAULT 'Active',
    CreatedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    FOREIGN KEY (AccountTypeID) REFERENCES DimAccountType(AccountTypeID)
);

CREATE TABLE FactTransaction (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    FromAccountID INT,
    ToAccountID INT,
    Amount DECIMAL(18,2),
    TransactionType VARCHAR(20),
    TransactionDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (FromAccountID) REFERENCES FactAccount(AccountID),
    FOREIGN KEY (ToAccountID) REFERENCES FactAccount(AccountID)
);

CREATE TABLE Audit_Log (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    Description VARCHAR(255),
    LoggedAt DATETIME DEFAULT GETDATE()
);

-- =========================
--  MODULES
-- =========================

-- KYC
CREATE TABLE CustomerKYC (
    CustomerID INT PRIMARY KEY,
    AadharNumber VARCHAR(12),
    PANNumber VARCHAR(10),
    Verified BIT DEFAULT 0,
    VerifiedDate DATETIME,
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

-- Joint Accounts
CREATE TABLE JointAccountMapping (
    AccountID INT,
    CustomerID INT,
    IsPrimaryHolder BIT,
    FOREIGN KEY (AccountID) REFERENCES FactAccount(AccountID),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

-- Debit Cards
CREATE TABLE DebitCard (
    CardNumber VARCHAR(16) PRIMARY KEY,
    CustomerID INT,
    AccountID INT,
    ExpiryDate DATE,
    CVV CHAR(3),
    Status VARCHAR(20),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    FOREIGN KEY (AccountID) REFERENCES FactAccount(AccountID)
);

CREATE TABLE CardTransaction (
    TransactionID INT IDENTITY(1,1) PRIMARY KEY,
    CardNumber VARCHAR(16),
    Amount DECIMAL(18,2),
    Merchant VARCHAR(100),
    TxnDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (CardNumber) REFERENCES DebitCard(CardNumber)
);

-- Loan + Repayment
CREATE TABLE LoanMaster (
    LoanID INT PRIMARY KEY,
    CustomerID INT,
    LoanType VARCHAR(50),
    PrincipalAmount DECIMAL(18,2),
    InterestRate DECIMAL(5,2),
    DurationMonths INT,
    StartDate DATE,
    Status VARCHAR(20),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

CREATE TABLE LoanRepayment (
    RepaymentID INT IDENTITY(1,1) PRIMARY KEY,
    LoanID INT,
    InstallmentNumber INT,
    DueDate DATE,
    PaidAmount DECIMAL(18,2),
    PaidDate DATE,
    FOREIGN KEY (LoanID) REFERENCES LoanMaster(LoanID)
);

-- Login & OTP
CREATE TABLE LoginAudit (
    CustomerID INT,
    LoginTime DATETIME,
    IPAddress VARCHAR(45),
    Success BIT,
    AttemptCount INT DEFAULT 1,
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

CREATE TABLE OTPLog (
    OTPID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    OTPCode VARCHAR(6),
    GeneratedAt DATETIME,
    ExpiryTime DATETIME,
    Used BIT DEFAULT 0,
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

-- Branch and Employee
CREATE TABLE DimBranch (
    BranchID INT PRIMARY KEY,
    BranchName VARCHAR(100),
    City VARCHAR(50),
    IFSCCode VARCHAR(20)
);

CREATE TABLE DimEmployee (
    EmployeeID INT PRIMARY KEY,
    Name VARCHAR(100),
    BranchID INT,
    Role VARCHAR(50),
    FOREIGN KEY (BranchID) REFERENCES DimBranch(BranchID)
);

CREATE TABLE TellerTransaction (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    EmployeeID INT,
    CustomerID INT,
    AccountID INT,
    Amount DECIMAL(18,2),
    TxnType VARCHAR(20),
    TxnDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (EmployeeID) REFERENCES DimEmployee(EmployeeID),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    FOREIGN KEY (AccountID) REFERENCES FactAccount(AccountID)
);

-- Security
CREATE TABLE SuspiciousActivity (
    ActivityID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    Reason VARCHAR(255),
    DetectedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

-- Notifications, Service Requests, Charges
CREATE TABLE Notification (
    NotificationID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    Message VARCHAR(255),
    SentAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

CREATE TABLE ServiceRequest (
    RequestID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    RequestType VARCHAR(50),
    Description VARCHAR(255),
    Status VARCHAR(20) DEFAULT 'Open',
    CreatedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

CREATE TABLE FeeCharges (
    ChargeID INT IDENTITY(1,1) PRIMARY KEY,
    AccountID INT,
    ChargeType VARCHAR(50),
    Amount DECIMAL(10,2),
    ChargedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (AccountID) REFERENCES FactAccount(AccountID)
);


-- ======================================
--  INDEXING
-- ======================================
 CREATE NONCLUSTERED INDEX idx_FactTransaction_DateType
ON FactTransaction (TransactionDate, TransactionType);

CREATE NONCLUSTERED INDEX idx_FactAccount_Customer
ON FactAccount (CustomerID);


-- ======================================
--  PROCEDURES
-- ======================================

-- ========== DEPOSIT MONEY ==========

GO;

CREATE PROCEDURE usp_Deposit @AccountID INT, @Amount DECIMAL(18,2)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE FactAccount
        SET Balance = Balance + @Amount
        WHERE AccountID = @AccountID;

        INSERT INTO FactTransaction (FromAccountID, ToAccountID, Amount, TransactionType)
        VALUES (NULL, @AccountID, @Amount, 'Deposit');

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        THROW;
    END CATCH
END;
GO;

-- ========== WITHDRAW MONEY ==========
CREATE PROCEDURE usp_Withdraw
    @AccountID INT,
    @Amount DECIMAL(18,2)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        IF (SELECT Balance FROM FactAccount WHERE AccountID = @AccountID) < @Amount
            THROW 50001, 'Insufficient Balance', 1;

        UPDATE FactAccount
        SET Balance = Balance - @Amount
        WHERE AccountID = @AccountID;

        INSERT INTO FactTransaction (FromAccountID, ToAccountID, Amount, TransactionType)
        VALUES (@AccountID, NULL, @Amount, 'Withdrawal');

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        THROW;
    END CATCH
END;
GO;
-- ========== TRANSFER MONEY ==========
CREATE PROCEDURE usp_TransferAmount
    @FromAccount INT,
    @ToAccount INT,
    @Amount DECIMAL(18,2)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        IF (SELECT Balance FROM FactAccount WHERE AccountID = @FromAccount) < @Amount
            THROW 50002, 'Insufficient Funds for Transfer', 1;

        UPDATE FactAccount
        SET Balance = Balance - @Amount
        WHERE AccountID = @FromAccount;

        UPDATE FactAccount
        SET Balance = Balance + @Amount
        WHERE AccountID = @ToAccount;

        INSERT INTO FactTransaction (FromAccountID, ToAccountID, Amount, TransactionType)
        VALUES (@FromAccount, @ToAccount, @Amount, 'Transfer');

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        THROW;
    END CATCH
END;
GO;
-- ========== GENERATE EMI SCHEDULE ==========
CREATE PROCEDURE usp_GenerateEMISchedule
    @LoanID INT
AS
BEGIN
    DECLARE @i INT = 1, @duration INT;
    SET @duration = (SELECT DurationMonths FROM LoanMaster WHERE LoanID = @LoanID);
    WHILE @i <= @duration
    BEGIN
        INSERT INTO LoanRepayment (LoanID, InstallmentNumber, DueDate, PaidAmount)
        VALUES (
            @LoanID,
            @i,
            DATEADD(MONTH, @i, (SELECT StartDate FROM LoanMaster WHERE LoanID = @LoanID)),
            0
        );
        SET @i += 1;
    END;
END;
GO;
--=========== VIEW TRANSACTION HISTORY ===============
CREATE PROCEDURE usp_ViewTransactions
    @AccountID INT
AS
BEGIN
    SELECT *
    FROM FactTransaction
    WHERE FromAccountID = @AccountID OR ToAccountID = @AccountID
    ORDER BY TransactionDate DESC;
END;
GO;

-- =========== CHECK BALANCE ===========
CREATE PROCEDURE usp_CheckBalance
    @AccountID INT
AS
BEGIN
    SELECT AccountID, Balance
    FROM FactAccount
    WHERE AccountID = @AccountID;
END;
GO;
--=========== VIEW CUSTOMER PROFILE =========== 
CREATE PROCEDURE usp_GetCustomerProfile
    @CustomerID INT
AS
BEGIN
    SELECT C.CustomerID, FirstName, LastName, Email, Phone,
           A.Street, A.City, A.State, A.ZipCode
    FROM DimCustomer C
    LEFT JOIN DimAddress A ON C.CustomerID = A.CustomerID
    WHERE C.CustomerID = @CustomerID;
END;
GO;

--=========== LIST ALL LOANS ===========
CREATE PROCEDURE usp_GetLoansByCustomer
    @CustomerID INT
AS
BEGIN
    SELECT * FROM LoanMaster
    WHERE CustomerID = @CustomerID;
END;
GO;

-- =========== TO MARK SERVICE REQUEST CLOSED =========
CREATE PROCEDURE usp_CloseServiceRequest
    @RequestID INT
AS
BEGIN
    UPDATE ServiceRequest
    SET Status = 'Closed'
    WHERE RequestID = @RequestID;
END;

GO;


-- ======================================
--  TRIGGERS
-- ======================================

-- ========== Trigger for balance change ==========
CREATE TRIGGER trg_AuditBalance
ON FactAccount
AFTER UPDATE
AS
BEGIN
    INSERT INTO Audit_Log (Description)
    SELECT CONCAT('Balance changed for AccountID: ', i.AccountID)
    FROM inserted i
    JOIN deleted d ON i.AccountID = d.AccountID
    WHERE i.Balance <> d.Balance;
END;
GO;
--========== Trigger to prevent withdrawal if balance goes negative ==========
CREATE TRIGGER trg_PreventNegativeBalance
ON FactAccount
AFTER UPDATE
AS
BEGIN
    IF EXISTS (
        SELECT 1
        FROM inserted
        WHERE Balance < 0
    )
    BEGIN
        RAISERROR('Negative balances are not allowed.', 16, 1);
        ROLLBACK TRANSACTION;
    END
END;
GO;
--========== Trigger to log login failures ==========
CREATE TRIGGER trg_LoginAuditFailure
ON LoginAudit
AFTER INSERT
AS
BEGIN
    INSERT INTO Notification (CustomerID, Message)
    SELECT CustomerID, 'Login failed from IP: ' + IPAddress
    FROM inserted
    WHERE Success = 0;
END;
GO;
--==========Trigger to archive closed accounts ==========
CREATE TABLE FactAccount_Archive (
    AccountID INT PRIMARY KEY,
    CustomerID INT,
    AccountTypeID INT,
    Balance DECIMAL(18,2),
    AccountStatus VARCHAR(20),
    ArchivedAt DATETIME DEFAULT GETDATE()
);

GO;

CREATE TRIGGER trg_ArchiveClosedAccounts
ON FactAccount
AFTER UPDATE
AS
BEGIN
    INSERT INTO FactAccount_Archive (AccountID, CustomerID, AccountTypeID, Balance, AccountStatus)
    SELECT d.AccountID, d.CustomerID, d.AccountTypeID, d.Balance, d.AccountStatus
    FROM deleted d
    JOIN inserted i ON d.AccountID = i.AccountID
    WHERE d.AccountStatus <> 'Closed' AND i.AccountStatus = 'Closed';
END;

GO;

-- ============================================
--  HANDLING SLOWLY CHANGING DIMENSIONS (SCD)
-- ============================================

-- ========== IMMUTABLE TABLE FOR SCD TYPE 1 =============
-- Enforce by disabling updates
CREATE TABLE DimCustomer_Immutable (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    CreatedAt DATETIME DEFAULT GETDATE()
);

DENY UPDATE ON DimCustomer_Immutable TO PUBLIC;

-- ========== UPDATE EMAIL USING SCD TYPE 2 ==============
CREATE TABLE DimCustomer_History (
    SurrogateKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    Email VARCHAR(100),
    EffectiveDate DATETIME,
    ExpiryDate DATETIME,
    IsCurrent BIT
);

GO;

CREATE PROCEDURE usp_UpdateEmail_SCD2
    @CustomerID INT,
    @NewEmail VARCHAR(100)
AS
BEGIN
    BEGIN TRANSACTION;
    UPDATE DimCustomer_History
    SET ExpiryDate = GETDATE(), IsCurrent = 0
    WHERE CustomerID = @CustomerID AND IsCurrent = 1;

    INSERT INTO DimCustomer_History (CustomerID, Email, EffectiveDate, ExpiryDate, IsCurrent)
    VALUES (@CustomerID, @NewEmail, GETDATE(), NULL, 1);
    COMMIT;
END;

--====== STORING PREVIOUS VERSIONS IN COLUMS FOR SCD TYPE 3========
ALTER TABLE DimCustomer
ADD PreviousEmail VARCHAR(100); 

-- Example update logic
UPDATE DimCustomer
SET PreviousEmail = Email,
    Email = 'new.email@example.com'
WHERE CustomerID = 1;

--===== STORING CURRENT AND HISTORY TABLES FOR SCD TYPE 4 ==========
CREATE TABLE DimCustomer_Current (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    Email VARCHAR(100),
    CreatedAt DATETIME DEFAULT GETDATE()
);

CREATE TABLE DimCustomer_HistoryOnly (
    SurrogateKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    Name VARCHAR(100),
    Email VARCHAR(100),
    ChangedAt DATETIME DEFAULT GETDATE()
);

-- Update logic
BEGIN TRANSACTION;
-- Archive current record
INSERT INTO DimCustomer_HistoryOnly (CustomerID, Name, Email)
SELECT CustomerID, Name, Email
FROM DimCustomer_Current
WHERE CustomerID = 1;

-- Update current table
UPDATE DimCustomer_Current
SET Email = 'updated@email.com'
WHERE CustomerID = 1;
COMMIT;

--===== PROCEDURE FOR UPDATING MAIL USING SCD TYPE 6 =========
CREATE TABLE DimCustomer_Type6 (
    SurrogateKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    Email VARCHAR(100),
    PreviousEmail VARCHAR(100),
    EffectiveDate DATETIME,
    ExpiryDate DATETIME,
    IsCurrent BIT
);

GO;

-- SCD Type 6 Update
CREATE PROCEDURE usp_UpdateEmail_SCD6
    @CustomerID INT,
    @NewEmail VARCHAR(100)
AS
BEGIN
    BEGIN TRANSACTION;

    DECLARE @OldEmail VARCHAR(100);
    SELECT @OldEmail = Email FROM DimCustomer_Type6 WHERE CustomerID = @CustomerID AND IsCurrent = 1;

    -- Invalidate current
    UPDATE DimCustomer_Type6
    SET ExpiryDate = GETDATE(), IsCurrent = 0
    WHERE CustomerID = @CustomerID AND IsCurrent = 1;

    -- Insert new with old email as previous
    INSERT INTO DimCustomer_Type6 (CustomerID, Email, PreviousEmail, EffectiveDate, IsCurrent)
    VALUES (@CustomerID, @NewEmail, @OldEmail, GETDATE(), 1);

    COMMIT;
END;



-- ======================================
--  TESTING 
-- ======================================

-- ========== TEST DATA ==========
INSERT INTO DimAccountType VALUES (1, 'Savings'), (2, 'Current');

INSERT INTO DimCustomer (CustomerID, FirstName, LastName, Email, Phone)
VALUES (1, 'John', 'Doe', 'john@example.com', '1234567890'),
       (2, 'Alice', 'Smith', 'alice@example.com', '9876543210');

INSERT INTO DimAddress VALUES (1, 1, '123 Main St', 'New York', 'NY', '10001');
INSERT INTO DimAddress VALUES (2, 2, '456 Elm St', 'Chicago', 'IL', '60601');

INSERT INTO FactAccount (AccountID, CustomerID, AccountTypeID, Balance)
VALUES (101, 1, 1, 1000.00),
       (102, 2, 1, 500.00);

INSERT INTO DimBranch VALUES (1, 'Main Branch', 'New York', 'IFSC0001');
INSERT INTO DimEmployee VALUES (1, 'Teller One', 1, 'Teller');

-- EMI Schedule Generation Test
INSERT INTO LoanMaster (LoanID, CustomerID, LoanType, PrincipalAmount, InterestRate, DurationMonths, StartDate, Status)
VALUES (1, 1, 'Home', 500000, 7.5, 12, GETDATE(), 'Active');
EXEC usp_GenerateEMISchedule @LoanID = 1;
SELECT * FROM LoanRepayment WHERE LoanID = 1;

-- Debit Card Test
INSERT INTO DebitCard VALUES ('1234567812345678', 1, 101, '2027-12-31', '123', 'Active');
INSERT INTO CardTransaction (CardNumber, Amount, Merchant)
VALUES ('1234567812345678', 1500.00, 'Amazon');
SELECT * FROM CardTransaction;

-- Teller Transaction Test
INSERT INTO TellerTransaction (EmployeeID, CustomerID, AccountID, Amount, TxnType)
VALUES (1, 1, 101, 250.00, 'Cash Deposit');
SELECT * FROM TellerTransaction;

-- Login & OTP Log Test
INSERT INTO LoginAudit (CustomerID, LoginTime, IPAddress, Success)
VALUES (1, GETDATE(), '192.168.1.1', 1);
INSERT INTO OTPLog (CustomerID, OTPCode, GeneratedAt, ExpiryTime)
VALUES (1, '123456', GETDATE(), DATEADD(MINUTE, 10, GETDATE()));
SELECT * FROM OTPLog;

-- Notification Test
INSERT INTO Notification (CustomerID, Message)
VALUES (1, 'Welcome to the Banking Portal!');
SELECT * FROM Notification;

-- Service Request Test
INSERT INTO ServiceRequest (CustomerID, RequestType, Description)
VALUES (1, 'Cheque Book', 'Need new cheque book for account');
SELECT * FROM ServiceRequest;

-- Charges Test
INSERT INTO FeeCharges (AccountID, ChargeType, Amount)
VALUES (101, 'Maintenance Fee', 99.99);
SELECT * FROM FeeCharges;

-- Suspicious Activity Log Test
INSERT INTO SuspiciousActivity (CustomerID, Reason)
VALUES (1, 'Multiple failed login attempts');
SELECT * FROM SuspiciousActivity;


-- ========== TEST PROCEDURES ==========
-- Deposit Test
EXEC usp_Deposit @AccountID = 101, @Amount = 200.00;

-- Withdraw Test
EXEC usp_Withdraw @AccountID = 101, @Amount = 150.00;

-- Transfer Test
EXEC usp_TransferAmount @FromAccount = 101, @ToAccount = 102, @Amount = 100.00;

-- Account transactions
EXEC usp_ViewTransactions @AccountId= 101;

-- Check balance
EXEC usp_CheckBalance @AccountID = 101;

-- Get customer profile
EXEC usp_GetCustomerProfile @CustomerID = 1;

-- Get loans for customer
EXEC usp_GetLoansByCustomer @CustomerID = 1;


-- Close service request
-- insert one
INSERT INTO ServiceRequest (CustomerID, RequestType, Description)
VALUES (2, 'Address Update', 'Please update address info');
-- Then close it
DECLARE @ReqID INT = (SELECT TOP 1 RequestID FROM ServiceRequest WHERE CustomerID = 2 ORDER BY CreatedAt DESC);
EXEC usp_CloseServiceRequest @RequestID = @ReqID;
SELECT * FROM ServiceRequest WHERE RequestID = @ReqID;


-- View Transactions
SELECT * FROM FactTransaction;

-- ========== TEST TRIGGER ==========

-- Balance Update should log audit
UPDATE FactAccount SET Balance = Balance + 50 WHERE AccountID = 101;
SELECT * FROM Audit_Log;

-- Prevent negative balance(MJST GIVE ERROR)
UPDATE FactAccount SET Balance = -500 WHERE AccountID = 101;

-- Log failed login and auto notification
INSERT INTO LoginAudit (CustomerID, LoginTime, IPAddress, Success)
VALUES (1, GETDATE(), '10.0.0.1', 0);
SELECT * FROM Notification WHERE CustomerID = 1;

--  Archive closed account and Mark account as closed
UPDATE FactAccount SET AccountStatus = 'Closed' WHERE AccountID = 102;
SELECT * FROM FactAccount_Archive WHERE AccountID = 102;

-- ========== TEST SCD ==========

-- SCD TYPE 0 CHECK (MUST GIVE ERROR)
UPDATE DimCustomer_Immutable SET Name = 'New Name' WHERE CustomerID = 1;

-- SCD TYPE 1: Overwrite
UPDATE DimCustomer
SET Email = 'type1@email.com'
WHERE CustomerID = 1;

-- SCD TYPE 2 CHECK: Email Update Test
INSERT INTO DimCustomer_History (CustomerID, Email, EffectiveDate, IsCurrent)
VALUES (1, 'john@example.com', GETDATE(), 1);
EXEC usp_UpdateEmail_SCD2 @CustomerID = 1, @NewEmail = 'john.new@example.com';
SELECT * FROM DimCustomer_History;

-- SCD TYPE 3 CHECK: Update email and store previous
UPDATE DimCustomer
SET PreviousEmail = Email,
    Email = 'type3@email.com'
WHERE CustomerID = 1;
SELECT CustomerID, Email, PreviousEmail FROM DimCustomer;

-- SCD TYPE 4 CHECK: Copy current to history then update
INSERT INTO DimCustomer_Current (CustomerID, Name, Email)
VALUES (3, 'Test User', 'test@original.com');

-- Archive old data
INSERT INTO DimCustomer_HistoryOnly (CustomerID, Name, Email)
SELECT CustomerID, Name, Email FROM DimCustomer_Current WHERE CustomerID = 3;

-- Update current
UPDATE DimCustomer_Current
SET Email = 'type4@new.com'
WHERE CustomerID = 3;

-- SCD TYPE 6 CHECK
-- Initial insert
INSERT INTO DimCustomer_Type6 (CustomerID, Email, PreviousEmail, EffectiveDate, IsCurrent)
VALUES (1, 'initial@email.com', NULL, GETDATE(), 1);

-- Update to new value
EXEC usp_UpdateEmail_SCD6 @CustomerID = 1, @NewEmail = 'hybrid@email.com';

-- Check history
SELECT * FROM DimCustomer_Type6 WHERE CustomerID = 1 ORDER BY EffectiveDate DESC;

--====== MONTHLY SUMMARY USING PIVOT ========
WITH MonthlyData AS (
    SELECT DATENAME(MONTH, TransactionDate) AS [Month], TransactionType, Amount
    FROM FactTransaction
)
SELECT *
FROM MonthlyData
PIVOT (
    SUM(Amount)
    FOR TransactionType IN ([Deposit], [Withdrawal], [Transfer])
) AS PivotResult;

-- ===== GROUPING SETS OF SUMMARY =======
SELECT TransactionType, COUNT(*) AS TxnCount, SUM(Amount) AS TotalAmount
FROM FactTransaction
GROUP BY GROUPING SETS (
    (TransactionType),
    ()
);

-- ====== AMOUNT SUM USING DYNAMIC SQL ======

DECLARE @ColList NVARCHAR(MAX) = 'TransactionType, SUM(Amount) AS TotalAmount';
DECLARE @Sql NVARCHAR(MAX) = 'SELECT ' + @ColList + ' FROM FactTransaction GROUP BY TransactionType';
EXEC sp_executesql @Sql;