/* =======================================================================
   Project   : Data Warehouse – Bronze Layer Table Creation
   File Name : ddl_bronze.sql
   Purpose   :
       This script creates the raw (Bronze Layer) tables in the
       DataWarehouse database. Bronze layer stores data exactly or
       almost exactly as it arrives from source systems.

       CRM Source Tables (Customer, Product, Sales):
         - bronze.crm_cust_info
         - bronze.crm_prd_info
         - bronze.crm_sales_details

       ERP Source Tables:
         - bronze.erp_cust_az12
         - bronze.erp_loc_a101
         - bronze.erp_px_cat_g1v2

   Key Points:
       - Tables are created ONLY if they don't already exist
       - No primary keys / foreign keys intentionally 
         (Bronze layer is raw storage)
       - Minimal transformations
       - Safe to run multiple times

   When to Run:
       - Initial DW setup
       - Recreating environment on a new server
       - Learning / practice setups

   Note:
       - Date fields in sales table are kept as INT because many systems
         store dates like YYYYMMDD in numeric format.
   ======================================================================= */


---------------------------------------------------------
-- Make sure we are in the correct database
---------------------------------------------------------
USE DataWarehouse;
GO


/* =======================================================
   1️⃣ CRM CUSTOMER INFORMATION
   ======================================================= */
IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'crm_cust_info' 
      AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    PRINT 'Creating table bronze.crm_cust_info...';

    CREATE TABLE bronze.crm_cust_info(
        cst_id              INT,
        cst_key             NVARCHAR(50),
        cst_firstname       NVARCHAR(50),
        cst_lastname        NVARCHAR(50),
        cst_marital_status  NVARCHAR(50),
        cst_gndr            NVARCHAR(50),
        cst_create_date     DATE
    );
END
ELSE
    PRINT 'Table bronze.crm_cust_info already exists';


/* =======================================================
   2️⃣ CRM PRODUCT INFORMATION
   ======================================================= */
IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'crm_prd_info' 
      AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    PRINT 'Creating table bronze.crm_prd_info...';

    CREATE TABLE bronze.crm_prd_info(
        prd_id       INT,
        prd_key      NVARCHAR(50),
        prd_nm       NVARCHAR(50),
        prd_cost     INT,
        prd_line     NVARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt   DATE
    );
END
ELSE
    PRINT 'Table bronze.crm_prd_info already exists';


/* =======================================================
   3️⃣ CRM SALES DETAILS
   ======================================================= */
IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'crm_sales_details' 
      AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    PRINT 'Creating table bronze.crm_sales_details...';

    CREATE TABLE bronze.crm_sales_details(
        sls_ord_num   NVARCHAR(50),
        sls_prd_key   NVARCHAR(50),
        sls_cust_id   INT,
        sls_order_dt  INT,   -- stored as integer YYYYMMDD
        sls_ship_dt   INT,
        sls_due_dt    INT,
        sls_sales     INT,
        sls_quantity  INT,
        sls_price     INT
    );
END
ELSE
    PRINT 'Table bronze.crm_sales_details already exists';


/* =======================================================
   4️⃣ ERP CUSTOMER TABLE
   ======================================================= */
IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'erp_cust_az12' 
      AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    PRINT 'Creating table bronze.erp_cust_az12...';

    CREATE TABLE bronze.erp_cust_az12(
        CID     NVARCHAR(50),
        BDATE   DATE,
        GEN     NVARCHAR(50)
    );
END
ELSE
    PRINT 'Table bronze.erp_cust_az12 already exists';


/* =======================================================
   5️⃣ ERP LOCATION TABLE
   ======================================================= */
IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'erp_loc_a101' 
      AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    PRINT 'Creating table bronze.erp_loc_a101...';

    CREATE TABLE bronze.erp_loc_a101(
        CID     NVARCHAR(50),
        CNTRY   NVARCHAR(50)
    );
END
ELSE
    PRINT 'Table bronze.erp_loc_a101 already exists';


/* =======================================================
   6️⃣ ERP PRODUCT CATEGORY TABLE
   ======================================================= */
IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'erp_px_cat_g1v2' 
      AND schema_id = SCHEMA_ID('bronze')
)
BEGIN
    PRINT 'Creating table bronze.erp_px_cat_g1v2...';

    CREATE TABLE bronze.erp_px_cat_g1v2(
        ID           NVARCHAR(50),
        CAT          NVARCHAR(50),
        SUBCAT       NVARCHAR(50),
        MAINTENANCE  NVARCHAR(50)
    );
END
ELSE
    PRINT 'Table bronze.erp_px_cat_g1v2 already exists';


PRINT 'All Bronze tables checked / created successfully 🎯';
