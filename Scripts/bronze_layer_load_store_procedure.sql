/*
-----------------------------------------------------------------------------------------
Procedure Name: bronze.load_bronze

Purpose:
This stored procedure performs a full refresh of the staging (bronze) layer by:
  - Truncating existing data in six staging tables
  - Loading new records from corresponding source CSV files using BULK INSERT

Source Files:
  - CRM: cust_info.csv, prd_info.csv, sales_details.csv
  - ERP: CUST_AZ12.csv, LOC_A101.csv, PX_CAT_G1V2.csv

Assumptions:
  - The CSV files exist at the specified paths
  - Each CSV includes a header row (skipped using FIRSTROW = 2)
  - The target tables are already created in the 'bronze' schema
  - Data is comma-separated and encoded in a compatible format

Warning:
  This operation is destructive. All existing data in the target tables will be deleted.
-----------------------------------------------------------------------------------------
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	PRINT '========================'
	PRINT 'Starting Bronze Layer Load'
	PRINT '========================'

	DECLARE @start_time DATETIME, @end_time DATETIME;

	-- CRM Data Load
	BEGIN TRY
		PRINT '========================'
		PRINT 'Loading the CRM Tables'
		PRINT '========================'

		-- Customer Info
		PRINT 'Loading bronze.crm_cust_info...'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\SQL-Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'crm_cust_info loaded successfully.';
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------'

		-- Product Info
		PRINT 'Loading bronze.crm_prd_info...'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\SQL-Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'crm_prd_info loaded successfully.';
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------'

		-- Sales Details
		PRINT 'Loading bronze.crm_sales_details...'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQL-Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'crm_sales_details loaded successfully.';
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT 'Error occurred while loading CRM tables.';
		PRINT ERROR_MESSAGE();
		RETURN;
	END CATCH

	-- ERP Data Load
	BEGIN TRY
		PRINT '========================'
		PRINT 'Loading the ERP Tables'
		PRINT '========================'

		-- Customer AZ12
		PRINT 'Loading bronze.erp_cust_az12...'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\SQL-Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'erp_cust_az12 loaded successfully.';
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------'

		-- Location A101
		PRINT 'Loading bronze.erp_loc_a101...'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\SQL-Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'erp_loc_a101 loaded successfully.';
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------'

		-- Product Category G1V2
		PRINT 'Loading bronze.erp_px_cat_g1v2...'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\SQL-Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'erp_px_cat_g1v2 loaded successfully.';
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT 'Error occurred while loading ERP tables.';
		PRINT ERROR_MESSAGE();
		RETURN;
	END CATCH

	PRINT '========================'
	PRINT 'Bronze Layer Load Complete'
	PRINT '========================'
END;
