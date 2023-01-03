use AdventureWorks2019

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

alter PROCEDURE dbo.Portfolio_SalesDashboard_Main
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

SELECT soh.[SalesOrderID]
      ,soh.[OrderDate]
      ,soh.[DueDate]
      ,soh.[ShipDate]
      ,soh.[OnlineOrderFlag]
	  ,p.BusinessEntityID 'SalespersonID'
	  ,p.LastName + ', ' + p.FirstName + ' [' + convert(varchar(50),p.BusinessEntityID) + ']' 'Salesperson'
      ,soh.[TerritoryID]
	  ,st.CountryRegionCode 'TerritoryCountryRegionCode'
	  ,st.Name
	  ,st.[Group] 'TerritoryContinent'
	  ,st.CountryRegionCode + ' - ' + st.Name + ' [' + convert(varchar(50),soh.[TerritoryID]) + ']' 'Territory'
	  ,c.StoreID
	  ,s.Name + ' [' + convert(varchar(50),c.StoreID) + ']' 'StoreName'
      ,soh.[SubTotal]
      ,soh.[TaxAmt]
      ,soh.[Freight]
      ,soh.[TotalDue]
  FROM [Sales].[SalesOrderHeader] soh
  inner join person.person p on p.BusinessEntityID = soh.SalesPersonID
  inner join sales.SalesTerritory st on st.TerritoryID = soh.TerritoryID
  inner join sales.Customer c on c.CustomerID = soh.CustomerID
  inner join sales.Store s on s.BusinessEntityID = c.StoreID
  where 0=0
	and soh.SalesPersonID is not null

END
GO
