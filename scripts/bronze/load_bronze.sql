create or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        set @batch_start_time = GETDATE();
        print '======================================================';
        print 'Loding Bronze Layer';
        print '======================================================';

        print '------------------------------------------------------';
        print 'Loding CRM Tables';
        print '------------------------------------------------------';
        
        set @start_time = getdate();
        print '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE table bronze.crm_cust_info;

        print '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        from "C:\01 My files\Curses\Projects\Data with Baraa SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
        with(
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
        print'----------------';

        set @start_time = getdate();
        print '>> Truncating Table: bronze.crm_prd_inf';
        TRUNCATE table bronze.crm_prd_inf;

        print '>> Inserting Data Into: bronze.crm_prd_inf';
        BULK INSERT bronze.crm_prd_inf
        from "C:\01 My files\Curses\Projects\Data with Baraa SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
        with(
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
        print'----------------';


        set @start_time = getdate();
        print '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE table bronze.crm_sales_details;

        print '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        from "C:\01 My files\Curses\Projects\Data with Baraa SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
        with(
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
        print'----------------';
        print '------------------------------------------------------';
        print 'Loding ERP Tables';
        print '------------------------------------------------------';


        set @start_time = getdate();
        print '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE table bronze.erp_cust_az12;

        print '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        from "C:\01 My files\Curses\Projects\Data with Baraa SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
        with(
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
        print'----------------';


        set @start_time = getdate();
        print '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE table bronze.erp_loc_a101;

        print '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        from "C:\01 My files\Curses\Projects\Data with Baraa SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
        with(
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
        print'----------------';


        set @start_time = getdate();
        print '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE table bronze.erp_px_cat_g1v2;

        print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        from "C:\01 My files\Curses\Projects\Data with Baraa SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
        with(
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
        print'----------------';
        
        set @batch_end_time = GETDATE();
        print'==========================================================';
        print'Loding BronzeLayer is Comleated';
        print'Loding duration :' + cast(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)as NVARCHAR) + 'Seconda'
        print'==========================================================';

    END TRY
    BEGIN CATCH
        print'==========================================================';
        print'ERROR OCCURED DURING LODING BRONZE LAYER';
        print'==========================================================';
    END CATCH
END
