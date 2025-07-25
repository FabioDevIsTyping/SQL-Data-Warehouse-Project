/*
Create Database and Schemas 

Script Purpose: 
This Script creates a new database called DataWarehouse after checking if it already exists.
If the database exits then it is dropped and recreated. The script also sets up three different schemas
called 'bronze', 'silver', 'gold' used for the medallion architecture in our database.

WARNING:
Running this script will entirely drop the 'DataWarehouse' database if it already exists AND dropIfExists is set to 1.
*/
USE master;
GO

DECLARE @dropIfExists BIT = 1;

IF EXISTS (SELECT name FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    IF @dropIfExists = 1
    BEGIN
        PRINT 'Dropping existing DataWarehouse...';
        ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE DataWarehouse;
    END
    ELSE
    BEGIN
        PRINT 'Database exists. Set @dropIfExists = 1 to allow dropping.';
        RETURN;
    END
END
GO

PRINT 'Creating DataWarehouse...';
CREATE DATABASE DataWarehouse;
GO

ALTER DATABASE DataWarehouse SET RECOVERY SIMPLE;
-- ALTER DATABASE DataWarehouse SET COMPATIBILITY_LEVEL = 150;
GO

USE DataWarehouse;
GO

PRINT 'Creating schemas...';
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO
