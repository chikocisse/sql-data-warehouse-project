/*
============================================================================================================================
Script DDL : Créer les Tables Bronze
============================================================================================================================
Objectif du Script :
Ce script crée des tables dans le schéma 'bronze', en supprimant les tables existantes si elles existent déjà.
Exécutez ce script pour redéfinir la structure DDL des tables 'bronze'.
Le texte original décrit un script de base de données destiné à créer ou recréer des tables 
dans un schéma appelé "bronze", qui fait probablement partie d'une architecture de données en couches (bronze, silver, gold)
couramment utilisée dans les projets de data engineering et de lakehouse.
================================================================================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR (50),
	cst_material_status NVARCHAR (50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
   prd_id       INT,
   prd_key      NVARCHAR(50),
   prd_nm       NVARCHAR(50),
   prd_cost     INT,
   prd_line     NVARCHAR(50),
   prd_start_dt DATETIME,
   prd_end_dt   DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
   sls_ord_num  NVARCHAR(50),
   sls_prd_key  NVARCHAR(50),
   sls_cust_id  INT,
   sls_order_dt INT,
   sls_ship_dt  INT,
   sls_due_dt   INT,
   sls_sales    INT,
   sls_quantity INT,
   sls_price    INT
);

IF OBJECT_ID ('bronze.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
   cid    NVARCHAR(50),
   cntry  NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
   cid    NVARCHAR(50),
   bdate  DATE,
   gen    NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
   id           NVARCHAR(50),
   cat          NVARCHAR(50),
   subcat       NVARCHAR(50),
   maintenance  NVARCHAR(50)
);





CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================================================================';
		PRINT 'Chargement du bronze';
		PRINT '=====================================================================================';

		PRINT '---------------------------------------------------------------------------------------';
		PRINT 'Chargement de la table CRM';
		PRINT '---------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Troncage de la table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Insertion des données dans: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info 
		FROM 'E:\DATASETS\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',', 
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>>TEMPS DE CHARGEMENT: '+ CAST (DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'seconds'; 
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Troncage de la table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Insertion des données dans la table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info 
		FROM 'E:\DATASETS\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>TEMPS DE CHARGEMENT: '+ CAST (DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'seconds'; 
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Troncage de la table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Insertion des données dans la table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		FROM 'E:\DATASETS\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>TEMPS DE CHARGEMENT: '+ CAST (DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'seconds'; 
		PRINT '-------------------';

		PRINT '---------------------------------------------------------------------------------------';
		PRINT 'Chargement de la table ERP';
		PRINT '---------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Troncage de la table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Insertion des données dans la table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12 
		FROM 'E:\DATASETS\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>TEMPS DE CHARGEMENT: '+ CAST (DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'seconds'; 
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Troncage de la table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Insertion des données dans la table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101 
		FROM 'E:\DATASETS\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>TEMPS DE CHARGEMENT: '+ CAST (DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'seconds'; 
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Troncage de la table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Insertion des données dans la table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2 
		FROM 'E:\DATASETS\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		
		PRINT '>>TEMPS DE CHARGEMENT: '+ CAST (DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'seconds'; 
		PRINT '-------------------';

		SET @batch_end_time = GETDATE();
		PRINT '==================================================';
		PRINT 'Le Chargement du  Bronze Layer est Complet';
		PRINT '    - Temps Total de Chargement: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==================================================';


	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERREUR RENCONTRÉE DURANT LE CHARGEMENT DU BRONZE';
		PRINT '=======================================================';
	END CATCH
END
GO

-- Exécution de la procédure stockée
EXEC bronze.load_bronze;
