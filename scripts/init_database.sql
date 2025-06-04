/*
============================================
create database and schemas
============================================
Script purpose:
	this script creates a new database named 'DataWareHouse' after checking if it already exists.
	if the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within the database: 'bronze', 'silver' and 'gold'
Warning:
	running this script will drop the entire 'DataWareHouse' database if it exists.
	all data in the database will be permanently deleted. proceed with caution
	and ensure you have proper backups before running this script.
*/
use master;
go

-- drop and recreate the DataWareHouse DB
if exists (select 1 from sys.databases where name = 'DataWareHouse')
begin
	alter database DataWareHouse set single_user with rollback immediate;
	drop database DataWareHouse;
end;
go

-- create the DB
create database DataWareHouse;
go

-- use the DB
use DataWareHouse;
go

-- create all schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
