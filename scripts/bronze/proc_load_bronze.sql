/*

**Procédure Stockée : Charger la Couche Bronze (Source -> Bronze)**

**Objectif du Script :**
Cette procédure stockée charge des données dans le schéma 'bronze' à partir de fichiers CSV externes.
Elle effectue les actions suivantes :
- Tronque les tables bronze avant de charger les données.
- Utilise la commande 'BULK INSERT' pour charger les données depuis les fichiers CSV vers les tables bronze.

**Paramètres :**
Aucun.
Cette procédure stockée n'accepte aucun paramètre et ne retourne aucune valeur.

**Exemple d'utilisation :**
EXEC bronze.load_bronze;

Le texte décrit une procédure stockée qui fait partie d'un pipeline de données ETL (Extract, Transform, Load),
spécifiquement pour charger des données brutes depuis des fichiers CSV vers 
la couche bronze d'une architecture de données en couches.
*/





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
