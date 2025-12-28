/*
=====================================================================================
Procedure Name: bronze.load_bronze
Database      : DataWarehouse
Author        : Anurag

Purpose:
    This stored procedure is designed to load all raw/source data files (CSV format)
    into the Bronze layer of the Data Warehouse. It handles bulk data insertion
    into all Bronze tables from their respective source CSV files. 

    The Bronze layer contains raw data directly ingested from CRM and ERP systems 
    without any transformation. This procedure is a crucial step in the ETL process.

Flow / Steps:
1. Captures the total start time of the Bronze load process.
2. Iterates through each Bronze table in a sequential order:
       a) Truncate the target table to remove previous data.
       b) Bulk insert data from the corresponding CSV file.
       c) Measure the start and end time for loading each table.
       d) Count the rows inserted and print a detailed status message.
       e) Wrap each table load in a TRY-CATCH block to handle errors without
          stopping the entire load process.
3. After all tables are loaded, captures the total end time.
4. Calculates the total time taken for the full Bronze layer load.
5. Prints detailed, readable, and indented messages to the console, including:
       - Step number and table name
       - Action performed (Truncate / Load)
       - Status of action
       - Rows inserted
       - Time taken for each table (in milliseconds)
       - Start and end timestamps for each table
       - Total time for the Bronze load

Tables Loaded:
    1. bronze.crm_cust_info        - Customer master data from CRM
    2. bronze.crm_prd_info         - Product master data from CRM
    3. bronze.crm_sales_details    - Sales transaction details from CRM
    4. bronze.erp_cust_az12        - Customer master data from ERP
    5. bronze.erp_loc_a101         - Location master data from ERP
    6. bronze.erp_px_cat_g1v2      - Product category master data from ERP

Key Features:
    - Uses BULK INSERT for fast loading of large CSV files.
    - Each table load has a TRY-CATCH block to log errors without interrupting
      the loading of remaining tables.
    - Prints well-indented and structured messages for easy reading.
    - Measures and prints time taken for each table and total execution time.
    - Provides row counts for validation.

Notes:
    - Ensure the CSV file paths are correct and accessible from the SQL Server.
    - The CSV files should have a header row, which is skipped (FIRSTROW = 2).
    - ROWTERMINATOR is set to '\n' for newline separation in CSV files.
    - FIELDTERMINATOR is set to ',' for comma-separated values.
    - TABLOCK hint is used to optimize bulk insert performance.
=====================================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @StepStart DATETIME,
        @StepEnd DATETIME,
        @TimeTakenMs INT,
        @RowsInserted INT,
        @TotalStart DATETIME,
        @TotalEnd DATETIME,
        @TotalTimeMs INT;

    -- Capture total start time
    SET @TotalStart = GETDATE();

    PRINT '=========================================================';
    PRINT '  STARTING BRONZE LAYER LOAD PROCESS';
    PRINT '  Start Time: ' + CONVERT(VARCHAR, @TotalStart, 120);
    PRINT '=========================================================';

    ------------------------------------------------------
    -- CRM Customer Info
    ------------------------------------------------------
    BEGIN TRY
        PRINT '';
        PRINT 'Step 1: bronze.crm_cust_info';
        PRINT '  Action : Truncate Table';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '  Status : Table truncated successfully';

        PRINT '  Action : Load CSV data from cust_info.csv';
        SET @StepStart = GETDATE();

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\dharm\OneDrive\Desktop\Data With Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @StepEnd = GETDATE();
        SET @TimeTakenMs = DATEDIFF(ms, @StepStart, @StepEnd);
        SELECT @RowsInserted = COUNT(*) FROM bronze.crm_cust_info;

        PRINT '  Status : Data loaded successfully';
        PRINT '  Rows Inserted : ' + CAST(@RowsInserted AS VARCHAR(20));
        PRINT '  Time Taken    : ' + CAST(@TimeTakenMs AS VARCHAR(20)) + ' ms';
        PRINT '  Completed at  : ' + CONVERT(VARCHAR, @StepEnd, 120);
    END TRY
    BEGIN CATCH
        PRINT '  ERROR : ' + ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------
    -- CRM Product Info
    ------------------------------------------------------
    BEGIN TRY
        PRINT '';
        PRINT 'Step 2: bronze.crm_prd_info';
        PRINT '  Action : Truncate Table';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '  Status : Table truncated successfully';

        PRINT '  Action : Load CSV data from prd_info.csv';
        SET @StepStart = GETDATE();

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\dharm\OneDrive\Desktop\Data With Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @StepEnd = GETDATE();
        SET @TimeTakenMs = DATEDIFF(ms, @StepStart, @StepEnd);
        SELECT @RowsInserted = COUNT(*) FROM bronze.crm_prd_info;

        PRINT '  Status : Data loaded successfully';
        PRINT '  Rows Inserted : ' + CAST(@RowsInserted AS VARCHAR(20));
        PRINT '  Time Taken    : ' + CAST(@TimeTakenMs AS VARCHAR(20)) + ' ms';
        PRINT '  Completed at  : ' + CONVERT(VARCHAR, @StepEnd, 120);
    END TRY
    BEGIN CATCH
        PRINT '  ERROR : ' + ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------
    -- CRM Sales Details
    ------------------------------------------------------
    BEGIN TRY
        PRINT '';
        PRINT 'Step 3: bronze.crm_sales_details';
        PRINT '  Action : Truncate Table';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '  Status : Table truncated successfully';

        PRINT '  Action : Load CSV data from sales_details.csv';
        SET @StepStart = GETDATE();

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\dharm\OneDrive\Desktop\Data With Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @StepEnd = GETDATE();
        SET @TimeTakenMs = DATEDIFF(ms, @StepStart, @StepEnd);
        SELECT @RowsInserted = COUNT(*) FROM bronze.crm_sales_details;

        PRINT '  Status : Data loaded successfully';
        PRINT '  Rows Inserted : ' + CAST(@RowsInserted AS VARCHAR(20));
        PRINT '  Time Taken    : ' + CAST(@TimeTakenMs AS VARCHAR(20)) + ' ms';
        PRINT '  Completed at  : ' + CONVERT(VARCHAR, @StepEnd, 120);
    END TRY
    BEGIN CATCH
        PRINT '  ERROR : ' + ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------
    -- ERP Customer
    ------------------------------------------------------
    BEGIN TRY
        PRINT '';
        PRINT 'Step 4: bronze.erp_cust_az12';
        PRINT '  Action : Truncate Table';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '  Status : Table truncated successfully';

        PRINT '  Action : Load CSV data from CUST_AZ12.csv';
        SET @StepStart = GETDATE();

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\dharm\OneDrive\Desktop\Data With Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @StepEnd = GETDATE();
        SET @TimeTakenMs = DATEDIFF(ms, @StepStart, @StepEnd);
        SELECT @RowsInserted = COUNT(*) FROM bronze.erp_cust_az12;

        PRINT '  Status : Data loaded successfully';
        PRINT '  Rows Inserted : ' + CAST(@RowsInserted AS VARCHAR(20));
        PRINT '  Time Taken    : ' + CAST(@TimeTakenMs AS VARCHAR(20)) + ' ms';
        PRINT '  Completed at  : ' + CONVERT(VARCHAR, @StepEnd, 120);
    END TRY
    BEGIN CATCH
        PRINT '  ERROR : ' + ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------
    -- ERP Location
    ------------------------------------------------------
    BEGIN TRY
        PRINT '';
        PRINT 'Step 5: bronze.erp_loc_a101';
        PRINT '  Action : Truncate Table';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '  Status : Table truncated successfully';

        PRINT '  Action : Load CSV data from LOC_A101.csv';
        SET @StepStart = GETDATE();

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\dharm\OneDrive\Desktop\Data With Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @StepEnd = GETDATE();
        SET @TimeTakenMs = DATEDIFF(ms, @StepStart, @StepEnd);
        SELECT @RowsInserted = COUNT(*) FROM bronze.erp_loc_a101;

        PRINT '  Status : Data loaded successfully';
        PRINT '  Rows Inserted : ' + CAST(@RowsInserted AS VARCHAR(20));
        PRINT '  Time Taken    : ' + CAST(@TimeTakenMs AS VARCHAR(20)) + ' ms';
        PRINT '  Completed at  : ' + CONVERT(VARCHAR, @StepEnd, 120);
    END TRY
    BEGIN CATCH
        PRINT '  ERROR : ' + ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------
    -- ERP Product Category
    ------------------------------------------------------
    BEGIN TRY
        PRINT '';
        PRINT 'Step 6: bronze.erp_px_cat_g1v2';
        PRINT '  Action : Truncate Table';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '  Status : Table truncated successfully';

        PRINT '  Action : Load CSV data from PX_CAT_G1V2.csv';
        SET @StepStart = GETDATE();

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\dharm\OneDrive\Desktop\Data With Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @StepEnd = GETDATE();
        SET @TimeTakenMs = DATEDIFF(ms, @StepStart, @StepEnd);
        SELECT @RowsInserted = COUNT(*) FROM bronze.erp_px_cat_g1v2;

        PRINT '  Status : Data loaded successfully';
        PRINT '  Rows Inserted : ' + CAST(@RowsInserted AS VARCHAR(20));
        PRINT '  Time Taken    : ' + CAST(@TimeTakenMs AS VARCHAR(20)) + ' ms';
        PRINT '  Completed at  : ' + CONVERT(VARCHAR, @StepEnd, 120);
    END TRY
    BEGIN CATCH
        PRINT '  ERROR : ' + ERROR_MESSAGE();
    END CATCH;

    ------------------------------------------------------
    -- Completed
    ------------------------------------------------------
    SET @TotalEnd = GETDATE();
    SET @TotalTimeMs = DATEDIFF(ms, @TotalStart, @TotalEnd);

    PRINT '=========================================================';
    PRINT '  BRONZE LAYER LOAD COMPLETED SUCCESSFULLY';
    PRINT '  Start Time : ' + CONVERT(VARCHAR, @TotalStart, 120);
    PRINT '  End Time   : ' + CONVERT(VARCHAR, @TotalEnd, 120);
    PRINT '  Total Time : ' + CAST(@TotalTimeMs AS VARCHAR(20)) + ' ms';
    PRINT '=========================================================';
END;
GO


-- EXEC bronze.load_bronze // This the way to exexute this above stored procedure

