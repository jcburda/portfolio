USE [CLARITY]
GO

/****** Object:  UserDefinedFunction [RW].[GrowthCurveLMS_CalculateZscorePercentile]    Script Date: 2/28/2024 12:11:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--alter FUNCTION [RW].[GrowthCurveLMS_CalculateZscorePercentile]
(
	@CurveSource_p varchar(500)
	,@CurveType_p varchar(500)
	,@CurveValue_p float
	,@MeasureValue_p float
	,@Sex_p char(1)
)
RETURNS 
TABLE

	RETURN 

	/*------------------------------------------------------------
	[jburda 2024-01-09]
		(1) Chose the right Curve Type. 
			- Each curve type name should have a suffix denoting the population for which it’s intended to be used (e.g. BMI_2To20Years is not for 0-23 month old patients). 
			- A suffix of “_Spliced” means the curve data is a combination of the “0-36 Month” curve and the “2-20 Year” curve. 
			- For all “_Spliced” curves, the 0-23 month data is always from the “0-36 Month” curve, and the 24-240 month data is always from the “2-20 Year” curve.
			- Note that sometimes these curves differ in their LMS values (and therefore calculated z-scores) during the overlap period.
		(2) MeasureValue units are unique to each Curve Source. 
			- The same measure in another growth curve may have different units (e.g. Fenton Preterm "weight" curve is in grams, not kilograms). See "CURVES, MEASURES, UNITS" for each growth curve.
		(3) Source table JOINs are tricky. 
			- If using the source tables instead of the main function, beware some JOIN / WHERE CONDITIONS may be unintuitive (e.g. the CDC Normal curve data is JOIN'd to with a BETWEEN instead of on a specific MeasureValue).
		(4) The 3 types of age. 
			- Gestational age = starts from first day of the woman's last menstrual period. 
			- Chronological age = starts from birth. 
			- Postmenstrual age = gestational age + chronological age.
			- For example, a baby delivered after 30 weeks of pregnancy who was immediately transferred after birth to the NICU for 10 days--> gestational age = 210 days (30 weeks x 7); chronological age = 10 days; postmenstrual age = 220 days (210 gestational + 10 chronological).
		(5) Percentile caveats. 
			- For percentiles, the LMS-calculation z-score is being converted to a NORMAL-distribution percentile rounded to the closest “05” percentile. 
			- Beware that this will not produce a 100% accurate percentile since none of these growth curves are perfectly NORMAL. 
			- Use these percentiles with discretion.

	[jburda 2024-03-04]
	- converted to "inline TVF" instead of "multi-statement TVF" (to thunderous, performance-tuned applause...)

	[jburda 2024-03-22]
	- add unit test proc; any additions to this function should have the "50th percentile" values add to that unit test proc
	------------------------------------------------------------*/

  
	/*------------------------------------------------------------
	Declare Variables
	------------------------------------------------------------*/
	--declare @Sex_p char(1) = 'M'
	--declare @CurveSource_p varchar(500) = 'CDC_Normal'
	--declare @CurveType_p varchar(500) = 'Weight_Spliced'
	--declare @CurveValue_p float = 16 --weight in kg
	--declare @MeasureValue_p float = 45.1 --chrono age in months
	
	select 
		a3.MeasureValue
		,a3.Zscore
		,case when a3.Zscore is null then null else a3.PercentileEstimate end 'PercentileEstimate'
	from (
		select
			a2.MeasureValue
			,a2.Zscore
			,(
				select
					a.PercentileEst
				from (
					select
						a2.Zscore 'Zscore'
						,b.Zscore 'Zscore2'
						,ABS(a2.Zscore - b.Zscore) 'AbsZscoreDiff'
						,b.Percentile
						,row_number() over(order by ABS(a2.Zscore - b.Zscore)) 'rn'
						,case 
							when convert(int, ROUND(b.Percentile / 5.0,0) * 5) = 0 then 1
							when convert(int, ROUND(b.Percentile / 5.0,0) * 5) = 100 then 99
							else convert(int, ROUND(b.Percentile / 5.0,0) * 5)
							end 'PercentileEst' --exclude 0 and 100 as percentiles
					from (select 1 'dummy') z --arbitrary, just needed for cross apply
					cross apply rw.PercentilesAndZscores_NormalDistribution b
				) a
				where 0=0
					and a.rn = 1
			) 'PercentileEstimate'
		from (
			select 
				case 
					when @CurveSource_p = 'CDC_Normal' then a1.CDC_Normal_MeasureValue
					when @CurveSource_p = 'CDC_DownSyndrome' then a1.CDC_DS_MeasureValue
					when @CurveSource_p = 'Fenton_Preterm' then a1.Fenton_Preterm_MeasureValue
					else null end 'MeasureValue'
				,case 
					when @CurveSource_p = 'CDC_Normal' then convert(decimal(20,8), ROUND(((POWER((@CurveValue_p / a1.CDC_Normal_Mu * 1.0), a1.CDC_Normal_Lambda) - 1) / (a1.CDC_Normal_Lambda * a1.CDC_Normal_Sigma * 1.0)) ,8))
					when @CurveSource_p = 'CDC_DownSyndrome' then convert(decimal(20,8), ROUND(((POWER((@CurveValue_p / a1.CDC_DS_Mu * 1.0), a1.CDC_DS_Lambda) - 1) / (a1.CDC_DS_Lambda * a1.CDC_DS_Sigma * 1.0)) ,8))
					when @CurveSource_p = 'Fenton_Preterm' then convert(decimal(20,8), ROUND(((POWER((@CurveValue_p / a1.Fenton_Preterm_Mu * 1.0), a1.Fenton_Preterm_Lambda) - 1) / (a1.Fenton_Preterm_Lambda * a1.Fenton_Preterm_Sigma * 1.0)) ,8))
					else NULL end 'Zscore'
			from (

				select distinct
					a.*
					,b.*
					,c.*
				from (select 1 'dummy') z --arbitrary, just needed for cross apply
				outer apply (
					select
						convert(varchar(50), a.MeasureValueLBI) + ' - ' + convert(varchar(50), a.MeasureValueUBI) 'CDC_Normal_MeasureValue'
						,a.Lambda 'CDC_Normal_Lambda'
						,a.Mu 'CDC_Normal_Mu'
						,a.Sigma 'CDC_Normal_Sigma'
					from rw.GrowthCurveLMS_CDC_Normal_BirthTo20Years a
					where 0=0
						and @Sex_p = a.Sex
						and @CurveType_p = a.CurveType
						and @MeasureValue_p between a.MeasureValueLBI and a.MeasureValueUBI
				) a
				outer apply (
					select
						convert(varchar(50), a.MeasureValue) 'CDC_DS_MeasureValue'
						,a.Lambda 'CDC_DS_Lambda'
						,a.Mu 'CDC_DS_Mu'
						,a.Sigma 'CDC_DS_Sigma'
					from (
						select 
							@MeasureValue_p 'MeasureValue_input'
							,a.MeasureValue
							,a.Lambda
							,a.Mu
							,a.Sigma
							,ABS(@MeasureValue_p - a.MeasureValue) 'AgeMonthsDiff'
							,row_number() over(order by ABS(@MeasureValue_p - a.MeasureValue)) 'rn'
						from rw.GrowthCurveLMS_CDC_DownSyndrome_BirthTo20Years a
						where 0=0
							and @Sex_p = a.Sex
							and @CurveType_p = a.CurveType
					) a
					where 0=0
						and a.rn = 1
				) b
				outer apply (
					select
						convert(varchar(50), a.MeasureValue) 'Fenton_Preterm_MeasureValue'
						,a.Lambda 'Fenton_Preterm_Lambda'
						,a.Mu 'Fenton_Preterm_Mu'
						,a.Sigma 'Fenton_Preterm_Sigma'
					from rw.GrowthCurveLMS_Fenton_Preterm_22To50Weeks a
					where 0=0
						and @Sex_p = a.Sex
						and @CurveType_p = a.CurveType
						and ROUND(@MeasureValue_p,0) = a.MeasureValue
				) c
			
			
			) a1
		
		) a2
	
	) a3 

	--[jburda 2024-03-22] unit test proc
	--exec [RW].[sp_GrowthCurveLMS_CalculateZscorePercentile_UnitTest]
	
GO