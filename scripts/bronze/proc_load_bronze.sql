create or alter procedure bronze.load_bronze as
	begin
		declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
		begin try
			set @batch_start_time = getdate();
			print '==================================================';
			print 'Loading Bronze Layer';
			print '==================================================';

			print '==================================================';
			print 'Loading CRM Tables';
			print '==================================================';
			set @start_time = GETDATE();
			truncate table bronze.crm_cust_info;
			bulk insert bronze.crm_cust_info
			from 'C:\Users\RONIN\Desktop\TUTORIALS\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with 
				(
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = GETDATE();
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @start_time, @end_time) as nvarchar);
			print '>> -----------------------------';
			-- **************
			set @start_time = GETDATE();
			truncate table bronze.crm_prd_info
			bulk insert bronze.crm_prd_info
			from 'C:\Users\RONIN\Desktop\TUTORIALS\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with
				(
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = GETDATE();
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @start_time, @end_time) as nvarchar);
			print '>> -----------------------------';
			-- **************
			set @start_time = GETDATE();
			truncate table bronze.crm_sales_details;
			bulk insert bronze.crm_sales_details
			from 'C:\Users\RONIN\Desktop\TUTORIALS\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with 
				(
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = GETDATE();
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @start_time, @end_time) as nvarchar);
			print '>> -----------------------------';
			-- ***************

			print '==================================================';
			print 'Loading ERP Tables';
			print '==================================================';
			set @start_time = GETDATE();
			truncate table bronze.erp_cust_az12;
			bulk insert bronze.erp_cust_az12
			from 'C:\Users\RONIN\Desktop\TUTORIALS\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			with 
				(
					firstrow = 2,
					fieldterminator = ',',
					tablock
				)
			set @end_time = GETDATE();
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @start_time, @end_time) as nvarchar);
			print '>> -----------------------------';
			-- ***************
			set @start_time = GETDATE();
			truncate table bronze.erp_loc_a101;
			bulk insert bronze.erp_loc_a101
			from 'C:\Users\RONIN\Desktop\TUTORIALS\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			with 
				(
					firstrow = 2,
					fieldterminator = ',',
					tablock
				)
			set @end_time = GETDATE();
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @start_time, @end_time) as nvarchar);
			print '>> -----------------------------';
			-- ***************
			set @start_time = GETDATE();
			truncate table bronze.erp_px_cat_g1v2;
			bulk insert bronze.erp_px_cat_g1v2
			from 'C:\Users\RONIN\Desktop\TUTORIALS\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			with 
				(
					firstrow = 2,
					fieldterminator = ',',
					tablock
				)
			set @end_time = GETDATE();
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @start_time, @end_time) as nvarchar);
			print '>> -----------------------------';

			set @batch_end_time = getdate();
			print '======================================================';
			print 'LOADING BRONZE LAYER IS COMPLETED :';
			print '>> LOAD DURATION IN SECOND : ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar);
			print '======================================================';
		end try
		begin catch
			print '======================================================';
			print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			print 'Error Message : ' + error_message();
			print 'Error Message number : ' + cast(error_number() as nvarchar);
			print 'Error Message state' + cast(error_state() as nvarchar);
			print '======================================================';
		end catch
	end
