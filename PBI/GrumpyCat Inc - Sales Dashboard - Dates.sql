use AdventureWorks2019

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

alter PROCEDURE dbo.Portfolio_SalesDashboard_Dates
	
AS
BEGIN
	
	SET NOCOUNT ON;

/*
Assumptions:
- today is 2014-05-01
- data is refreshed in the morning
- all data is one day behind
- 9/1 is the start of the fiscal year
*/

declare @today date = '2014-05-01'
declare @startdate date = '2011-05-31'
declare @enddate date = '2014-05-01'
--------------------------------------------------------
-- get date dimension
--------------------------------------------------------
drop table if exists #dates
create table #dates (dt date, MonthDisplay varchar(50), FQ varchar(2), FY varchar(4), QuickDate varchar(50))

declare @start date = @startdate
declare @end date = @enddate

while @start <= @end
BEGIN

	declare @target date = @start

	insert into #dates 
	select 
		@target
		,case 
			when month(@target) IN (9, 12, 3, 6) then '   ' + datename(month, @target) --trick to properly sort months in the hierarchy
			when month(@target) IN (10, 1, 4, 7) then '  ' + datename(month, @target)
			when month(@target) IN (11, 2, 5, 8) then ' ' + datename(month, @target)
		End 'MonthDisplay'
		,case
			when month(@target) IN (9, 10, 11) then 'Q1'
			when month(@target) IN (12, 1, 2) then 'Q2'
			when month(@target) IN (3, 4, 5) then 'Q3'
			when month(@target) IN (6, 7, 8) then 'Q4'
		end 'FQ'
		,case
			when month(@target) IN (9, 10, 11, 12) then 'FY' + RIGHT(year(@target),2)
			else 'FY' + RIGHT(year(dateadd(year, -1, @target)),2)
		end 'FY'
		,NULL

	set @start = dateadd(day, 1, @start)

END

drop table if exists #final
create table #final (dt date, MonthDisplay varchar(50), FY varchar(50), FQ varchar(50), Fiscal varchar(50), QuickDate varchar(50))
insert into #final
select 
	d.dt
	,d.MonthDisplay
	,d.FY
	,d.FQ
	,d.FY + d.FQ
	,'    T-1'
from #dates d
where 0=0
	and d.dt between @startdate and @enddate 
	and d.dt = dateadd(day, -1, @today)

union

select 
	d.dt
	,d.MonthDisplay
	,d.FY
	,d.FQ
	,d.FY + d.FQ
	,'   M-1'
from #dates d
where 0=0
	and d.dt between @startdate and @enddate
	and datefromparts(year(d.dt),month(d.dt),1) = datefromparts(year(dateadd(month, -1, @EndDate)),month(dateadd(month, -1, @EndDate)),1)

union

select 
	d.dt
	,d.MonthDisplay
	,d.FY
	,d.FQ
	,d.FY + d.FQ
	,'  MTD'
from #dates d
where 0=0
	and d.dt between @startdate and @enddate 
	and DATEFROMPARTS(YEAR(@today),Month(@today),1) = DATEFROMPARTS(YEAR(d.dt),MONTH(d.dt),1)

union

select 
	d.dt
	,d.MonthDisplay
	,d.FY
	,d.FQ
	,d.FY + d.FQ
	,' FQTD'
from #dates d
where 0=0
	and d.dt between @startdate and @enddate 
	AND dbo.CalculateFiscalQuarterAndYear(d.dt, 'Q') = dbo.CalculateFiscalQuarterAndYear(@EndDate, 'Q') 
	AND dbo.CalculateFiscalQuarterAndYear(d.dt, 'Y') = dbo.CalculateFiscalQuarterAndYear(@EndDate, 'Y') 

union

select 
	d.dt
	,d.MonthDisplay
	,d.FY
	,d.FQ
	,d.FY + d.FQ
	,'FYTD'
from #dates d
where 0=0
	and d.dt between @startdate and @enddate 
	AND dbo.CalculateFiscalQuarterAndYear(d.dt, 'Y') = dbo.CalculateFiscalQuarterAndYear(@EndDate, 'Y')

--select * from #dates

insert into #final
select 
	d.dt
	,d.MonthDisplay
	,d.FY
	,d.FQ
	,d.FY + d.FQ
	,NULL
from #dates d
where 0=0
	and d.dt < '2013-09-01'

select * 
from #final 
order by dt desc




/*

CREATE FUNCTION dbo.CalculateFiscalQuarterAndYear
(
	@dt date
	,@param1 varchar(50)
)
RETURNS varchar(50)
AS
BEGIN
	-- Declare the return variable here
	DECLARE @result varchar(50)

	if @param1 = 'Q'
	begin
		set @result =
			case
				when month(@dt) in (9, 10, 11) then 'Q1'
				when month(@dt) in (12, 1, 2) then 'Q2'
				when month(@dt) in (3, 4, 5) then 'Q3'
				else 'Q4'
			end
	END

	if @param1 = 'Y'
	begin
		set @result =
			case
				when month(@dt) in (9, 10, 11, 12) then 'FY' + RIGHT(year(@dt),2)
				else 'FY' + RIGHT(year(dateadd(year,-1,@dt)),2)
			end
	end

	-- Return the result of the function
	RETURN @result

END
GO

*/

----------------------------------------------------------------------------------------------------------------------------------

END
GO
