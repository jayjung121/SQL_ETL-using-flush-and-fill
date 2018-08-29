--*************************************************************************--
-- Title: Assignment02
-- Author: <ByungSu Jung>
-- Desc: This file tests you knowlege on how to create a ETL process with SQL code
-- Change Log: When,Who,What
-- 2018-01-17,<ByungSu Jung>,Created File

-- Instructions: 
-- (STEP 1) Create a lite version of the Northwind database by running the provided code.
-- (STEP 2) Create a new Data Warehouse called DWNorthwindLite based on the NorthwindLite DB.
--          The DW should have three dimension tables (for Customers, Products, and Dates) and one fact table.
-- (STEP 3) Fill the DW by creating an ETL Script
--**************************************************************************--
USE [DWNorthwindLite];
go
SET NoCount ON;
go
	If Exists(Select * from Sys.objects where Name = 'pETLDropForeignKeyConstraints')
   Drop Procedure pETLDropForeignKeyConstraints;
go
	If Exists(Select * from Sys.objects where Name = 'pETLTruncateTables')
   Drop Procedure pETLTruncateTables;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProducts')
   Drop View vETLDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimProducts')
   Drop Procedure pETLFillDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimCustomers')
   Drop View vETLDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimCustomers')
   Drop Procedure pETLFillDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactOrders')
   Drop View vETLFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillFactOrders')
   Drop Procedure pETLFillFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLAddForeignKeyConstraints')
   Drop Procedure pETLAddForeignKeyConstraints;

--********************************************************************--
-- A) Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--
go

Create Procedure pETLDropForeignKeyConstraints
/* Author: <ByungSu Jung>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Alter Table [DWNorthwindLite].dbo.FactOrders
	  Drop Constraint [fkFactOrdersToDimProducts]; 
	
	Alter Table DWNorthwindLite.dbo.FactOrders
	  Drop Constraint fkFactOrdersToDimCustomers

    -- Optional: Unlike the other tables DimDates does not change often --
    Alter Table [DWNorthwindLite].dbo.FactOrders
	   Drop Constraint [fkFactOrdersToDimDates];
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropForeignKeyConstraints;
 Print @Status;
*/
go

Create Procedure pETLTruncateTables
/* Author: <ByungSu Jung>
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Truncate Table [DWNorthwindLite].dbo.DimProducts;
	Truncate Table [dbo].[DimCustomers]
	Truncate Table [dbo].[FactOrders]
    -- Optional: Unlike the other tables DimDates does not change often --
    Truncate Table [DWNorthwindLite].dbo.DimDates; 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLTruncateTables;
 Print @Status;
*/
go

--********************************************************************--
-- B) FILL the Tables
--********************************************************************--
/****** [dbo].[DimProducts] ******/
go 

Create View vETLDimProducts
/* Author: <ByungSu Jung>
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
  FROM [NorthwindLite].dbo.Categories as c
  INNER JOIN [NorthwindLite].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLFillDimProducts
/* Author: <ByungSu Jung>
** Desc: Inserts data into DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    IF ((Select Count(*) From DimProducts) = 0)
     Begin
      INSERT INTO [DWNorthwindLite].dbo.DimProducts
      ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [ProductID]
       ,[ProductName]
       ,[ProductCategoryID]
       ,[ProductCategoryName]
       ,[StartDate] = -1
       ,[EndDate] = Null -- Default
       ,[IsCurrent] = 'Yes' -- Default
      FROM vETLDimProducts
    End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimProducts;
 Print @Status;
 Select * From DimProducts;
*/


/****** [dbo].[DimCustomers] ******/
go 
Create View vETLDimCustomers
/* Author: <ByungSu Jung>
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
As
  SELECT
	[CustomerID] = c.CustomerID, 
	[CustomerName] = CAST(c.CompanyName as nVarchar(100)), 
	[CustomerCity] = CAST(c.City as nVarchar(100)),
	[CustomerCountry] = CAST(c.Country as nVarchar(100))
  From NorthwindLite.dbo.Customers as c
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLFillDimCustomers
/* Author: <ByungSu Jung>
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
   Insert into DWNorthwindLite.dbo.DimCustomers
    ([CustomerID], [CustomerName], [CustomerCity], [CustomerCountry], [StartDate], [EndDate], [IsCurrent])
   Select
	[CustomerID], 
	[CustomerName], 
	[CustomerCity], 
	[CustomerCountry], 
	[StartDate] = -1, 
	[EndDate] = Null, 
	[IsCurrent] = 'Yes'
   From vETLDimCustomers
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimCustomers;
 Print @Status;
 Select * From DimCustomers;
*/
go

/****** [dbo].[DimDates] ******/
Create Procedure pETLFillDimDates
/* Author: <ByungSu Jung>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/1990'
      Declare @EndDate datetime = '12/31/1999' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       Insert Into DimDates 
       ( [DateKey], [USADateName], [MonthKey], [MonthName], [QuarterKey], [QuarterName], [YearKey], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthKey]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Cast(DateName(YYYY,@DateInProcess) + '0' + (DateName(quarter, @DateInProcess) ) as int)  -- [QuarterKey]
        ,'Q' + DateName(quarter, @DateInProcess) + ' - ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
        ,Year(@DateInProcess) -- [YearKey] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
*/
go

/****** [dbo].[FactOrders] ******/
go 
Create View vETLFactOrders
/* Author: <ByungSu Jung>
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
As
  SELECT
   [OrderID] = o.OrderID,
   [CustomerKey] = dc.CustomerKey, 
   [OrderDateKey] = dd.DateKey, 
   [ProductKey] = p.ProductKey, 
   [ActualOrderUnitPrice] = od.UnitPrice, 
   [ActualOrderQuantity] = od.Quantity
  From NorthwindLite.dbo.OrderDetails as od
  Join NorthwindLite.dbo.Orders as o
  On od.OrderID = o.OrderID
  Join DWNorthwindLite.dbo.DimCustomers as dc
  On o.CustomerID = dc.CustomerID
  Join DWNorthwindLite.dbo.DimDates as dd
  On Cast(Convert(nVarchar(50), o.OrderDate, 112) as int) = dd.DateKey
  Join DWNorthwindLite.dbo.DimProducts as p
  On od.ProductID = p.ProductID
go


Create Procedure pETLFillFactOrders
/* Author: <ByungSu Jung>
** Desc: Inserts data into FactOrders
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
   Insert Into DWNorthwindLite.dbo.FactOrders
    ([OrderID], [CustomerKey], [OrderDateKey], [ProductKey], [ActualOrderUnitPrice], [ActualOrderQuantity])
   Select 
    OrderID,
	CustomerKey,
	OrderDateKey,
	ProductKey,
	ActualOrderUnitPrice,
	ActualOrderQuantity
   From vETLFactOrders
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/*
 Declare @Status int;
 Exec @Status = pETLFillFactOrders;
 Print @Status;
  Select * From FactOrders;

go
*/
--********************************************************************--
-- C) Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
go
Create Procedure pETLAddForeignKeyConstraints
/* Author: <ByungSu Jung>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 20189-01-17,<ByungSu Jung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
      ADD CONSTRAINT fkFactOrdersToDimProducts
      FOREIGN KEY (ProductKey) REFERENCES DimProducts(ProductKey);
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
	 ADD CONSTRAINT fkFactOrdersToDimCustomers
	 FOREIGN KEY (CustomerKey) REFERENCES DimCustomers(CustomerKey)
    -- Optional: Unlike the other tables DimDates does not change often --
    ALTER TABLE DWNorthwindLite.dbo.FactOrders
      ADD CONSTRAINT fkFactOrdersToDimDates 
      FOREIGN KEY (OrderDateKey) REFERENCES DimDates(DateKey);
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLAddForeignKeyConstraints;
 Print @Status;
*/
go

--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int;
Exec @Status = pETLDropForeignKeyConstraints;
Select [Object] = 'pETLDropForeignKeyConstraints', [Status] = @Status;

Exec @Status = pETLTruncateTables;
Select [Object] = 'pETLTruncateTables', [Status] = @Status;

Exec @Status = pETLFillDimProducts;
Select [Object] = 'pETLFillDimProducts', [Status] = @Status;

Exec @Status = pETLFillDimCustomers;
Select [Object] = 'pETLFillDimCustomers', [Status] = @Status;

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = @Status;

Exec @Status = pETLFillFactOrders;
Select [Object] = 'pETLFillFactOrders', [Status] = @Status;

Exec @Status = pETLAddForeignKeyConstraints;
Select [Object] = 'pETLAddForeignKeyConstraints', [Status] = @Status;

go
Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactOrders];