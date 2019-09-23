USE [Elmer]
GO

/****** Object:  StoredProcedure [HHSurvey].[mergeHouseholdDim2014]    Script Date: 9/12/2019 3:56:36 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create proc [HHSurvey].[mergeHouseholdDim2017]
as 
BEGIN

	;WITH cte AS 
	(
	SELECT 
		  CAST([hhid] as bigint) as HouseholdDimID
			,[hhid] AS [hhid]
			,[hhgroupValue] AS [hhgroup]
			,[sample_segmentValue] AS [sample_segment]
			,[sample_county] AS [sample_county]
			,[final_cntyValue] AS [final_cnty]
			,[cityofredmondValue] AS [cityofredmond]
			,[cityofseattleValue] AS [cityofseattle]
			,[psrcValue] AS [psrc]
			,[final_home_tract] AS [final_home_tract]
			,[final_home_bg] AS [final_home_bg]
			,[final_home_puma10] AS [final_home_puma10]
			,[final_home_rgcnum] AS [final_home_rgcnum]
			,[final_home_uvnum] AS [final_home_uvnum]
			,[final_home_taz2010] AS [final_home_taz2010]
			,[travelweek] AS [travelweek]
			,[traveldate] AS [traveldate]
			,[dayofweekValue] AS [dayofweek]
			,[hhsizeValue] AS [hhsize]
			,[vehicle_countValue] AS [vehicle_count]
			,[numadults] AS [numadults]
			,[numchildren] AS [numchildren]
			,[numworkers] AS [numworkers]
			,[lifecycleValue] AS [lifecycle]
			,[hhincome_detailedValue] AS [hhincome_detailed]
			,[hhincome_followupValue] AS [hhincome_followup]
			,[hhincome_broadValue] AS [hhincome_broad]
			,[car_shareValue] AS [car_share]
			,[rent_ownValue] AS [rent_own]
			,[res_durValue] AS [res_dur]
			,[res_typeValue] AS [res_type]
			,[res_monthsValue] AS [res_months]
			,[offparkValue] AS [offpark]
			,[offpark_cost] AS [offpark_cost]
			,[streetparkValue] AS [streetpark]
			,[prev_home_waValue] AS [prev_home_wa]
			,[prev_home_tract] AS [prev_home_tract]
			,[prev_home_bg] AS [prev_home_bg]
			,[prev_home_puma10] AS [prev_home_puma10]
			,[prev_home_rgcname] AS [prev_home_rgcname]
			,[prev_home_taz2010] AS [prev_home_taz2010]
			,[prev_home_notwa_notusValue] AS [prev_home_notwa_notus]
			,[prev_home_notwa_city] AS [prev_home_notwa_city]
			,[prev_home_notwa_zip] AS [prev_home_notwa_zip]
			,[prev_home_notwa_state] AS [prev_home_notwa_state]
			,[prev_rent_ownValue] AS [prev_rent_own]
			,[prev_res_typeValue] AS [prev_res_type]
			,[res_factors_30minValue] AS [res_factors_30min]
			,[res_factors_affordValue] AS [res_factors_afford]
			,[res_factors_closefamValue] AS [res_factors_closefam]
			,[res_factors_hhchangeValue] AS [res_factors_hhchange]
			,[res_factors_hwyValue] AS [res_factors_hwy]
			,[res_factors_schoolValue] AS [res_factors_school]
			,[res_factors_spaceValue] AS [res_factors_space]
			,[res_factors_transitValue] AS [res_factors_transit]
			,[res_factors_walkValue] AS [res_factors_walk]
			,[rmove_optinValue] AS [rmove_optin]
			,[diary_incentive_typeValue] AS [diary_incentive_type]
			,[extra_incentiveValue] AS [extra_incentive]
			,[call_centerValue] AS [call_center]
			,[mobile_deviceValue] AS [mobile_device]
			,[contact_emailValue] AS [contact_email]
			,[contact_phoneValue] AS [contact_phone]
			,[foreign_languageValue] AS [foreign_language]
			,[google_translateValue] AS [google_translate]
			,[recruit_start_pt] AS [recruit_start_pt]
			,[recruit_end_pt] AS [recruit_end_pt]
			,[recruit_duration_min] AS [recruit_duration_min]
			,[numdayscompleteValue] AS [numdayscomplete]
			,[day1completeValue] AS [day1complete]
			,[day2completeValue] AS [day2complete]
			,[day3completeValue] AS [day3complete]
			,[day4completeValue] AS [day4complete]
			,[day5completeValue] AS [day5complete]
			,[day6completeValue] AS [day6complete]
			,[day7completeValue] AS [day7complete]
			,[num_trips] AS [num_trips]
			,[nwkdaysValue] AS [nwkdays]
			,[hh_wt_revised] AS [hh_wt_revised]
			,[hh_day_wt_revised] AS [hh_day_wt_revised]
		  ,BINARY_CHECKSUM(*) AS DWCHECKSUM
	  FROM [stg].[ResponseAndCodeHousehold_2017]
	)
	MERGE HHSurvey.HouseholdDim2017 AS target
	USING cte AS source
	ON
	(
		target.HouseholdDimID = source.HouseholdDimID
	)
	WHEN MATCHED AND COALESCE(target.DWCHECKSUM,-1) <> COALESCE(source.DWCHECKSUM, -1) THEN
		UPDATE SET
		target.[hhid] = source.[hhid]
		,target.[hhgroup] = source.[hhgroup]
		,target.[sample_segment] = source.[sample_segment]
		,target.[sample_county] = source.[sample_county]
		,target.[final_cnty] = source.[final_cnty]
		,target.[cityofredmond] = source.[cityofredmond]
		,target.[cityofseattle] = source.[cityofseattle]
		,target.[psrc] = source.[psrc]
		,target.[final_home_tract] = source.[final_home_tract]
		,target.[final_home_bg] = source.[final_home_bg]
		,target.[final_home_puma10] = source.[final_home_puma10]
		,target.[final_home_rgcnum] = source.[final_home_rgcnum]
		,target.[final_home_uvnum] = source.[final_home_uvnum]
		,target.[final_home_taz2010] = source.[final_home_taz2010]
		,target.[travelweek] = source.[travelweek]
		,target.[traveldate] = source.[traveldate]
		,target.[dayofweek] = source.[dayofweek]
		,target.[hhsize] = source.[hhsize]
		,target.[vehicle_count] = source.[vehicle_count]
		,target.[numadults] = source.[numadults]
		,target.[numchildren] = source.[numchildren]
		,target.[numworkers] = source.[numworkers]
		,target.[lifecycle] = source.[lifecycle]
		,target.[hhincome_detailed] = source.[hhincome_detailed]
		,target.[hhincome_followup] = source.[hhincome_followup]
		,target.[hhincome_broad] = source.[hhincome_broad]
		,target.[car_share] = source.[car_share]
		,target.[rent_own] = source.[rent_own]
		,target.[res_dur] = source.[res_dur]
		,target.[res_type] = source.[res_type]
		,target.[res_months] = source.[res_months]
		,target.[offpark] = source.[offpark]
		,target.[offpark_cost] = source.[offpark_cost]
		,target.[streetpark] = source.[streetpark]
		,target.[prev_home_wa] = source.[prev_home_wa]
		,target.[prev_home_tract] = source.[prev_home_tract]
		,target.[prev_home_bg] = source.[prev_home_bg]
		,target.[prev_home_puma10] = source.[prev_home_puma10]
		,target.[prev_home_rgcname] = source.[prev_home_rgcname]
		,target.[prev_home_taz2010] = source.[prev_home_taz2010]
		,target.[prev_home_notwa_notus] = source.[prev_home_notwa_notus]
		,target.[prev_home_notwa_city] = source.[prev_home_notwa_city]
		,target.[prev_home_notwa_zip] = source.[prev_home_notwa_zip]
		,target.[prev_home_notwa_state] = source.[prev_home_notwa_state]
		,target.[prev_rent_own] = source.[prev_rent_own]
		,target.[prev_res_type] = source.[prev_res_type]
		,target.[res_factors_30min] = source.[res_factors_30min]
		,target.[res_factors_afford] = source.[res_factors_afford]
		,target.[res_factors_closefam] = source.[res_factors_closefam]
		,target.[res_factors_hhchange] = source.[res_factors_hhchange]
		,target.[res_factors_hwy] = source.[res_factors_hwy]
		,target.[res_factors_school] = source.[res_factors_school]
		,target.[res_factors_space] = source.[res_factors_space]
		,target.[res_factors_transit] = source.[res_factors_transit]
		,target.[res_factors_walk] = source.[res_factors_walk]
		,target.[rmove_optin] = source.[rmove_optin]
		,target.[diary_incentive_type] = source.[diary_incentive_type]
		,target.[extra_incentive] = source.[extra_incentive]
		,target.[call_center] = source.[call_center]
		,target.[mobile_device] = source.[mobile_device]
		,target.[contact_email] = source.[contact_email]
		,target.[contact_phone] = source.[contact_phone]
		,target.[foreign_language] = source.[foreign_language]
		,target.[google_translate] = source.[google_translate]
		,target.[recruit_start_pt] = source.[recruit_start_pt]
		,target.[recruit_end_pt] = source.[recruit_end_pt]
		,target.[recruit_duration_min] = source.[recruit_duration_min]
		,target.[numdayscomplete] = source.[numdayscomplete]
		,target.[day1complete] = source.[day1complete]
		,target.[day2complete] = source.[day2complete]
		,target.[day3complete] = source.[day3complete]
		,target.[day4complete] = source.[day4complete]
		,target.[day5complete] = source.[day5complete]
		,target.[day6complete] = source.[day6complete]
		,target.[day7complete] = source.[day7complete]
		,target.[num_trips] = source.[num_trips]
		,target.[nwkdays] = source.[nwkdays]
		,target.[hh_wt_revised] = source.[hh_wt_revised]
		,target.[hh_day_wt_revised] = source.[hh_day_wt_revised]
	WHEN NOT MATCHED THEN INSERT (
		  HouseholdDimID
		,[hhid]
		,[hhgroup]
		,[sample_segment]
		,[sample_county]
		,[final_cnty]
		,[cityofredmond]
		,[cityofseattle]
		,[psrc]
		,[final_home_tract]
		,[final_home_bg]
		,[final_home_puma10]
		,[final_home_rgcnum]
		,[final_home_uvnum]
		,[final_home_taz2010]
		,[travelweek]
		,[traveldate]
		,[dayofweek]
		,[hhsize]
		,[vehicle_count]
		,[numadults]
		,[numchildren]
		,[numworkers]
		,[lifecycle]
		,[hhincome_detailed]
		,[hhincome_followup]
		,[hhincome_broad]
		,[car_share]
		,[rent_own]
		,[res_dur]
		,[res_type]
		,[res_months]
		,[offpark]
		,[offpark_cost]
		,[streetpark]
		,[prev_home_wa]
		,[prev_home_tract]
		,[prev_home_bg]
		,[prev_home_puma10]
		,[prev_home_rgcname]
		,[prev_home_taz2010]
		,[prev_home_notwa_notus]
		,[prev_home_notwa_city]
		,[prev_home_notwa_zip]
		,[prev_home_notwa_state]
		,[prev_rent_own]
		,[prev_res_type]
		,[res_factors_30min]
		,[res_factors_afford]
		,[res_factors_closefam]
		,[res_factors_hhchange]
		,[res_factors_hwy]
		,[res_factors_school]
		,[res_factors_space]
		,[res_factors_transit]
		,[res_factors_walk]
		,[rmove_optin]
		,[diary_incentive_type]
		,[extra_incentive]
		,[call_center]
		,[mobile_device]
		,[contact_email]
		,[contact_phone]
		,[foreign_language]
		,[google_translate]
		,[recruit_start_pt]
		,[recruit_end_pt]
		,[recruit_duration_min]
		,[numdayscomplete]
		,[day1complete]
		,[day2complete]
		,[day3complete]
		,[day4complete]
		,[day5complete]
		,[day6complete]
		,[day7complete]
		,[num_trips]
		,[nwkdays]
		,[hh_wt_revised]
		,[hh_day_wt_revised]
	)
	VALUES
	(
		source.HouseholdDimID
		,source.[hhid]
		,source.[hhgroup]
		,source.[sample_segment]
		,source.[sample_county]
		,source.[final_cnty]
		,source.[cityofredmond]
		,source.[cityofseattle]
		,source.[psrc]
		,source.[final_home_tract]
		,source.[final_home_bg]
		,source.[final_home_puma10]
		,source.[final_home_rgcnum]
		,source.[final_home_uvnum]
		,source.[final_home_taz2010]
		,source.[travelweek]
		,source.[traveldate]
		,source.[dayofweek]
		,source.[hhsize]
		,source.[vehicle_count]
		,source.[numadults]
		,source.[numchildren]
		,source.[numworkers]
		,source.[lifecycle]
		,source.[hhincome_detailed]
		,source.[hhincome_followup]
		,source.[hhincome_broad]
		,source.[car_share]
		,source.[rent_own]
		,source.[res_dur]
		,source.[res_type]
		,source.[res_months]
		,source.[offpark]
		,source.[offpark_cost]
		,source.[streetpark]
		,source.[prev_home_wa]
		,source.[prev_home_tract]
		,source.[prev_home_bg]
		,source.[prev_home_puma10]
		,source.[prev_home_rgcname]
		,source.[prev_home_taz2010]
		,source.[prev_home_notwa_notus]
		,source.[prev_home_notwa_city]
		,source.[prev_home_notwa_zip]
		,source.[prev_home_notwa_state]
		,source.[prev_rent_own]
		,source.[prev_res_type]
		,source.[res_factors_30min]
		,source.[res_factors_afford]
		,source.[res_factors_closefam]
		,source.[res_factors_hhchange]
		,source.[res_factors_hwy]
		,source.[res_factors_school]
		,source.[res_factors_space]
		,source.[res_factors_transit]
		,source.[res_factors_walk]
		,source.[rmove_optin]
		,source.[diary_incentive_type]
		,source.[extra_incentive]
		,source.[call_center]
		,source.[mobile_device]
		,source.[contact_email]
		,source.[contact_phone]
		,source.[foreign_language]
		,source.[google_translate]
		,source.[recruit_start_pt]
		,source.[recruit_end_pt]
		,source.[recruit_duration_min]
		,source.[numdayscomplete]
		,source.[day1complete]
		,source.[day2complete]
		,source.[day3complete]
		,source.[day4complete]
		,source.[day5complete]
		,source.[day6complete]
		,source.[day7complete]
		,source.[num_trips]
		,source.[nwkdays]
		,source.[hh_wt_revised]
		,source.[hh_day_wt_revised]
	);

END
GO


