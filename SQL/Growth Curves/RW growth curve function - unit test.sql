Use clarity 

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
[jburda 2024-03-22] unit testing for rw.GrowthCurveLMS_CalculateZscorePercentile (and my sanity)
*/

--alter PROCEDURE [RW].[sp_GrowthCurveLMS_CalculateZscorePercentile_UnitTest]
--AS
--BEGIN

	SET NOCOUNT ON;

	drop table if exists #all_data

	-- CDC Normal - Percentile 25
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValueLBI 'MeasureValue'
		,a.Percentile25 'CurveValue'
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'25' 'IntendedPercentileEstimate'
	into #all_data
	from (
		select
			'CDC_NORMAL' 'CurveSource'
			,a.CurveType
			,a.Percentile25
			,a.MeasureValueLBI
			,a.Sex
		from rw.GrowthCurveLMS_CDC_Normal_BirthTo20Years a
		where 0=0
			and a.MeasureValueLBI = 30
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile25, a.MeasureValueLBI, a.Sex) b

	union

	-- CDC Normal - Percentile 50
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValueLBI 'MeasureValue'
		,a.Percentile50
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'50'
	from (
		select
			'CDC_NORMAL' 'CurveSource'
			,a.CurveType
			,a.Percentile50
			,a.MeasureValueLBI
			,a.Sex
		from rw.GrowthCurveLMS_CDC_Normal_BirthTo20Years a
		where 0=0
			and a.MeasureValueLBI = 30
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile50, a.MeasureValueLBI, a.Sex) b

	union 

	-- CDC Normal - Percentile 75
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValueLBI 'MeasureValue'
		,a.Percentile75
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'75'
	from (
		select
			'CDC_NORMAL' 'CurveSource'
			,a.CurveType
			,a.Percentile75
			,a.MeasureValueLBI
			,a.Sex
		from rw.GrowthCurveLMS_CDC_Normal_BirthTo20Years a
		where 0=0
			and a.MeasureValueLBI = 30
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile75, a.MeasureValueLBI, a.Sex) b

	--------------------------------------------------------------------

	union

	-- CDC DS - Percentile 25
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.Percentile25
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'25'
	from (
		select
			'CDC_DownSyndrome' 'CurveSource'
			,a.CurveType
			,a.Percentile25
			,a.MeasureValue
			,a.Sex
		from rw.GrowthCurveLMS_CDC_DownSyndrome_BirthTo20Years a
		where 0=0
			and a.MeasureValue = 30
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile25, a.MeasureValue, a.Sex) b

	union

	-- CDC DS - Percentile 50
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.Percentile50
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'50'
	from (
		select
			'CDC_DownSyndrome' 'CurveSource'
			,a.CurveType
			,a.Percentile50
			,a.MeasureValue
			,a.Sex
		from rw.GrowthCurveLMS_CDC_DownSyndrome_BirthTo20Years a
		where 0=0
			and a.MeasureValue = 30
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile50, a.MeasureValue, a.Sex) b

	union

	-- CDC DS - Percentile 75
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.Percentile75
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'75'
	from (
		select
			'CDC_DownSyndrome' 'CurveSource'
			,a.CurveType
			,a.Percentile75
			,a.MeasureValue
			,a.Sex
		from rw.GrowthCurveLMS_CDC_DownSyndrome_BirthTo20Years a
		where 0=0
			and a.MeasureValue = 30
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile75, a.MeasureValue, a.Sex) b

	------------------------------------------------------------------

	union

	-- Fenton_Preterm - Percentile 10
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.Percentile10
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'10'
	from (
		select
			'Fenton_Preterm' 'CurveSource'
			,a.CurveType
			,a.Percentile10
			,a.MeasureValue
			,a.Sex
		from rw.GrowthCurveLMS_Fenton_Preterm_22To50Weeks a
		where 0=0
			and a.MeasureValue = 250
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile10, a.MeasureValue, a.Sex) b

	union

	-- Fenton_Preterm - Percentile 50
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.Percentile50
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'50'
	from (
		select
			'Fenton_Preterm' 'CurveSource'
			,a.CurveType
			,a.Percentile50
			,a.MeasureValue
			,a.Sex
		from rw.GrowthCurveLMS_Fenton_Preterm_22To50Weeks a
		where 0=0
			and a.MeasureValue = 250
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile50, a.MeasureValue, a.Sex) b

	union

	-- Fenton_Preterm - Percentile 90
	select
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.Percentile90
		,a.Sex
		,b.Zscore
		,b.PercentileEstimate
		,'90'
	from (
		select
			'Fenton_Preterm' 'CurveSource'
			,a.CurveType
			,a.Percentile90
			,a.MeasureValue
			,a.Sex
		from rw.GrowthCurveLMS_Fenton_Preterm_22To50Weeks a
		where 0=0
			and a.MeasureValue = 250
	) a
	outer apply rw.GrowthCurveLMS_CalculateZscorePercentile(a.CurveSource, a.CurveType, a.Percentile90, a.MeasureValue, a.Sex) b

	--[jburda 2024-03-22] add below UNION to simulate failure
	/*
	union

	select
		'Testing1'
		,'HeadCir'
		,30
		,90
		,'M'
		,-0.04
		,51
		,'50'

	union

	select
		'Testing2'	
		,'Height'
		,30
		,90
		,'M'
		,-0.06
		,49
		,'50'

	union

	select
		'Testing3'	
		,'Weight'
		,30
		,90
		,'M'
		,-0.06
		,55
		,'50'
	--*/

	/*------------------------------------------------------------
	unit test logic
	------------------------------------------------------------*/

	drop table if exists #results
	select 
		a.CurveSource
		,a.CurveType
		,a.MeasureValue
		,a.CurveValue
		,a.Sex
		,convert(decimal(20,4), a.Zscore) 'Zscore'
		,convert(decimal(20,4), b.Zscore) 'IntendedZscore'
		,convert(decimal(20,4), (ABS(a.Zscore - b.Zscore))) 'ZscoreDiff'
		,a.PercentileEstimate 'PercentileEstimate'
		,a.IntendedPercentileEstimate
		,convert(decimal(20,4), ABS(a.PercentileEstimate - a.IntendedPercentileEstimate)) 'PercentileDiff'
		,case when ABS(a.Zscore - b.Zscore) >= .05 then 1 else 0 end 'IsErrorWithZscore'
		,case when a.PercentileEstimate <> a.IntendedPercentileEstimate then 1 else 0 end 'IsErrorWithPercentile'
	into #results
	from #all_data a
	outer apply (
		select
			z.Zscore
		from rw.PercentilesAndZscores_NormalDistribution z
		where 0=0
			and z.Percentile = a.IntendedPercentileEstimate
	) b
	

	select * from #results a
	where 0=0
		and (a.IsErrorWithZscore = 1 or a.IsErrorWithPercentile = 1)
	order by
		a.CurveSource
		,a.CurveType
		,a.Sex

	--/* return all results too
	select * from #results a
	where 0=0
		--and (a.IsErrorWithZscore = 1 or a.IsErrorWithPercentile = 1)
	order by
		a.CurveSource
		,a.CurveType
		,a.Sex
	--*/

	-----------------------		

	declare @NewLine CHAR(2) = CHAR(13) + CHAR(10)
	declare @unitTestCount varchar(5) = (select COUNT(*) from #results)
	declare @unitTestsPassed varchar(5) = (select COUNT(*) from #results where IsErrorWithZscore = 0 and IsErrorWithPercentile = 0)
	declare @unitTestsFailed varchar(5) = (select COUNT(*) from #results where IsErrorWithZscore = 1 or IsErrorWithPercentile = 1)
	declare @unitTestsFailedNames varchar(MAX) = (
		select STRING_AGG('[' + z.CurveSource + ' - ' + z.CurveType + ' - ' + z.Sex + ' - ' + z.IntendedPercentileEstimate + ']','; ') within group (order by z.CurveSource, z.CurveType, z.Sex) 
		from #results z
		where 0=0
			and (z.IsErrorWithZscore = 1 or z.IsErrorWithPercentile = 1)
	)

	declare @errorMessage_Succss varchar(MAX) = 'SUCCESS! ' + @NewLine + 'Tests Run: ' + @unitTestCount + @NewLine + 'Tests Passed: ' + @unitTestsPassed + + @NewLine + 'Tests Failed: ' + @unitTestsFailed
	declare @errorMessage_Failure varchar(MAX) = 'FAILURE! ' + @NewLine + 'Tests Run: ' + @unitTestCount + @NewLine + 'Tests Passed: ' + @unitTestsPassed + + @NewLine + 'Tests Failed: ' + @unitTestsFailed
		+ @NewLine + @unitTestsFailedNames

	IF (select SUM(z.IsErrorWithZscore) from #results z) > 0 or (select SUM(z.IsErrorWithPercentile) from #results z) > 0
	begin
	   ;THROW 51000, @errorMessage_Failure, 1
	end
	else 
	begin
		;THROW 51000, @errorMessage_Succss, 1
	end


END
GO