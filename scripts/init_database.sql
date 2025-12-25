/* =======================================================================
   Project   : Data Warehouse
   File Name : init_database.sql
   Author    : Anurag Dharmadhiakari
   Purpose   :
       This script creates the DataWarehouse database and sets up the
       standard multi-layer architecture used in Data Warehousing:
       
       1) bronze  – Raw data layer (directly loaded from source systems)
       2) silver  – Cleaned, transformed, and standardized data
       3) gold    – Final reporting, analytics, dashboards, BI layer

   Key Points:
       - Safe to run multiple times (checks before creating objects)
       - Does NOT drop anything
       - Prints status messages to track what is happening
       - Keeps script simple but follow good SQL habits

   When to Run:
       - First time setting up the DW environment
       - When deploying to a new SQL Server instance
       - When rebuilding development / learning environment

   Notes:
       - Does not configure advanced settings like file locations,
         recovery model, or permissions (on purpose to keep it simple)
   ======================================================================= */

SET NOCOUNT ON;
GO

-------------------------------------------------------
-- 1. Create Database If It Does Not Already Exist
-------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 
    FROM sys.databases 
    WHERE name = 'DataWarehouse'
)
BEGIN
    PRINT 'Creating DataWarehouse database...';
    CREATE DATABASE DataWarehouse;
END
ELSE
BEGIN
    PRINT 'Database already exists. Skipping creation.';
END
GO

-------------------------------------------------------
-- 2. Switch to DataWarehouse Database
-------------------------------------------------------
USE DataWarehouse;
GO
PRINT 'Using DataWarehouse database';

-------------------------------------------------------
-- 3. Create Bronze / Silver / Gold Schemas
-------------------------------------------------------

-- Bronze Layer (Raw Data)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT 'Bronze schema created';
END
ELSE
BEGIN
    PRINT 'Bronze schema already exists';
END

-- Silver Layer (Cleaned + Processed Data)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT 'Silver schema created';
END
ELSE
BEGIN
    PRINT 'Silver schema already exists';
END

-- Gold Layer (Analytics / Reporting Ready Data)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT 'Gold schema created';
END
ELSE
BEGIN
    PRINT 'Gold schema already exists';
END

PRINT 'Data Warehouse setup completed successfully 🎯';
