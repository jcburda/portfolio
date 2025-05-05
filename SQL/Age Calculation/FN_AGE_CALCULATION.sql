USE REDACTED
GO
/****** Object:  UserDefinedFunction [RW].[FN_AGE_CALCULATION]    Script Date: 3/18/2024 10:56:31 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
NOTES
[jburda 2024-03-04] created function
[jburda 2024-03-06] converted to inline TVF for way better performance


OVERVIEW
This function is designed to help calculate various types of patient ages (gestational = GA; chronological = CA; corrected-gestational = CGA; post-menstrual = PMA) using only PAT_ID, a compare date, and weeks considered "full term".
Patients without a gestational age in Epic will have NULL values for CGA and PMA. 
To use the default for @p_FullTerm automatically, use the keyword "DEFAULT" in the third parameter position when calling the function. 
Sample usage of the function is at the end of the definition.


PARAMETERS							| DEFINITION
-----------------------------------------------------------------------------------------------
@p_PatId varchar(50)				| same as PATIENT.PAT_ID
@p_ComparisonDateTime datetime		| the date against which the age will be calculated
@p_FullTerm int = 40				| weeks considered "full term"; defaulted to 40; used for corrected-gestational age


COLUMNS								| DEFINITION
-----------------------------------------------------------------------------------------------
PAT_ID								| 
BIRTH_DATE							|
COMPARISON_DATE						| same as @p_ComparisonDateTime; the date against which age is calculated
IS_MISSING_BIRTH_DATE				| boolean for whether PATIENT.BIRTH_DATE is NULL
IS_MISSING_GA						| boolean for whether PATIENT_3.PED_GEST_AGE_DAYS is NULL
FULL_TERM_DAYS						| same as @p_FullTerm but in days
GA_DAYS								| gestational age in days (rounded to closest day)
GA_DAYS_EARLY						| difference between CA_DAYS and CGA_DAYS (rounded to closest day)
CA_DAYS								| chronological age in days (rounded to closest day)
CGA_DAYS							| corrected-gestational age in days (rounded to closest day)
PMA_DAYS							| post-menstrual age in days (rounded to closest day)
FULL_TERM_WEEKS						| same as @p_FullTerm
GA_WEEKS							| 
GA_WEEKS_FRACTION					| PATIENT.PED_GEST_AGE
CA_WEEKS							|
CGA_WEEKS							|
PMA_WEEKS							|
CA_MONTHS							|
CGA_MONTHS							|
PMA_MONTHS							|
CA_YEARS							|
PMA_YEARS							|
									|
*/

ALTER FUNCTION [RW].[FN_AGE_CALCULATION]
(
	@p_PatId varchar(50)
	,@p_ComparisonDateTime datetime
	,@p_FullTerm int = 40
)
RETURNS TABLE
AS
return 

	--declare @p_PatId varchar(50) = REDACTED
	--declare @p_FullTerm int = 38
	--declare @p_ComparisonDateTime datetime = getdate()

	select
		a.PAT_ID
		,a.BIRTH_DATE
		,a.COMPARISON_DATE
		,a.IS_MISSING_BIRTH_DATE
		,a.IS_MISSING_GA
		-----------
		,a.FULL_TERM_WEEKS * 7 'FULL_TERM_DAYS'
		,a.GA_DAYS
		,(a.FULL_TERM_WEEKS * 7) - a.GA_DAYS 'GA_DAYS_EARLY'
		,a.CA_DAYS
		--,a.CGA_DIFF_DAYS
		,(a.CA_DAYS - a.CGA_DIFF_DAYS) 'CGA_DAYS'
		,(a.CA_DAYS + a.GA_DAYS) 'PMA_DAYS'
		-----------------
		,a.FULL_TERM_WEEKS
		,convert(decimal(20,2), a.GA_DAYS / 7.0) 'GA_WEEKS'
		,a.GA_WEEKS_FRACTION
		--,a.FullTermWeeks - convert(decimal(20,2), a.GA_DAYS / 7.0) 'GaWeeksEarly'
		,convert(decimal(20,2), a.CA_DAYS / 7.0) 'CA_WEEKS'
		--,convert(decimal(20,2), a.CGA_DIFF_DAYS / 7.0) 'CgaDiffWeeks'
		,convert(decimal(20,2), (a.CA_DAYS - a.CGA_DIFF_DAYS) / 7.0) 'CGA_WEEKS'
		,convert(decimal(20,2), (a.CA_DAYS + a.GA_DAYS) / 7.0) 'PMA_WEEKS'
		-----------
		,convert(decimal(20,2), a.CA_DAYS / 30.4167) 'CA_MONTHS'
		,convert(decimal(20,2), (a.CA_DAYS - a.CGA_DIFF_DAYS) / 30.4167) 'CGA_MONTHS'
		,convert(decimal(20,2), (a.CA_DAYS + a.GA_DAYS) / 30.4167) 'PMA_MONTHS'
		-----------
		,convert(decimal(20,2), a.CA_DAYS / 365.25) 'CA_YEARS'
		,convert(decimal(20,2), (a.CA_DAYS + a.GA_DAYS) / 365.25) 'PMA_YEARS'
	from (
		select 
			a.PAT_ID
			,a.BIRTH_DATE
			,a.COMPARISON_DATE
			,a.FULL_TERM_WEEKS
			,a.IS_MISSING_BIRTH_DATE
			,a.IS_MISSING_GA
			---------
			,convert(decimal(20,0), datediff(minute, a.BIRTH_DATE, @p_ComparisonDateTime) / 60 / 24.0) 'CA_DAYS'
			,a.PED_GEST_AGE_DAYS 'GA_DAYS'
			,a.PED_GEST_AGE 'GA_WEEKS_FRACTION'
			,convert(decimal(20,0), ((@p_FullTerm * 7) - a.PED_GEST_AGE_DAYS)) 'CGA_DIFF_DAYS'
		from (
			select
				p.PAT_ID
				,p.BIRTH_DATE
				,p.PED_GEST_AGE
				,p3.PED_GEST_AGE_DAYS
				,@p_ComparisonDateTime 'COMPARISON_DATE'
				,@p_FullTerm 'FULL_TERM_WEEKS'
				,case when p.BIRTH_DATE is null then 1 else 0 end 'IS_MISSING_BIRTH_DATE'
				,case when p3.PED_GEST_AGE_DAYS is null then 1 else 0 end 'IS_MISSING_GA'
				----------------
				--a.PAT_ID
				--,a.BIRTH_DATE
				--,a.PED_GEST_AGE
				--,a.PED_GEST_AGE_DAYS
				--,@p_ComparisonDateTime 'COMPARISON_DATE'
				--,@p_FullTerm 'FULL_TERM_WEEKS'
				--,case when a.BIRTH_DATE is null then 1 else 0 end 'IS_MISSING_BIRTH_DATE'
				--,case when a.PED_GEST_AGE_DAYS is null then 1 else 0 end 'IS_MISSING_GA'
			from patient p
			inner join PATIENT_3 p3 on p3.PAT_ID = p.PAT_ID
			--from orgfilter.patient p
			--inner join orgfilter.PATIENT_3 p3 on p3.PAT_ID = p.PAT_ID
			--from rw.[V_FN_AGE_CALCULATION_PATIENT_INFO] a
			where 0=0
				and p.PAT_ID = @p_PatId
		) a
	) a