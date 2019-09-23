USE [Elmer]
GO

/****** Object:  StoredProcedure [HHSurvey].[mergePersonDim2014]    Script Date: 9/12/2019 3:56:36 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


alter proc [HHSurvey].[mergePersonDim2017]
as 
BEGIN

	;WITH cte AS 
	(
	SELECT 
		  CAST([personid] as bigint) as PersonDimID
		,[hhid] AS [hhid]
		,[personid] AS [personid]
		,[pernum] AS [pernum]
		,[sample_segmentValue] AS [sample_segment]
		,[hhgroupValue] AS [hhgroup]
		,[traveldate] AS [traveldate]
		,[relationshipValue] AS [relationship]
		,[proxy_parentValue] AS [proxy_parent]
		,[proxyValue] AS [proxy]
		,[ageValue] AS [age]
		,[genderValue] AS [gender]
		,[employmentValue] AS [employment]
		,[jobs_countValue] AS [jobs_count]
		,[workerValue] AS [worker]
		,[studentValue] AS [student]
		,[schooltypeValue] AS [schooltype]
		,[educationValue] AS [education]
		,[licenseValue] AS [license]
		,[vehicleusedValue] AS [vehicleused]
		,[smartphone_typeValue] AS [smartphone_type]
		,[smartphone_ageValue] AS [smartphone_age]
		,[smartphone_qualifiedValue] AS [smartphone_qualified]
		,[race_afamValue] AS [race_afam]
		,[race_aiakValue] AS [race_aiak]
		,[race_asianValue] AS [race_asian]
		,[race_hapiValue] AS [race_hapi]
		,[race_hispValue] AS [race_hisp]
		,[race_whiteValue] AS [race_white]
		,[race_otherValue] AS [race_other]
		,[race_noanswerValue] AS [race_noanswer]
		,[workplaceValue] AS [workplace]
		,[hours_workValue] AS [hours_work]
		,[commute_freqValue] AS [commute_freq]
		,[commute_modeValue] AS [commute_mode]
		,[commute_durValue] AS [commute_dur]
		,[telecommute_freqValue] AS [telecommute_freq]
		,[wpktypValue] AS [wpktyp]
		,[workpassValue] AS [workpass]
		,[workpass_cost] AS [workpass_cost]
		,[workpass_cost_dkValue] AS [workpass_cost_dk]
		,[work_county] AS [work_county]
		,[work_tract] AS [work_tract]
		,[work_bg] AS [work_bg]
		,[work_puma10] AS [work_puma10]
		,[work_rgcname] AS [work_rgcname]
		,[work_taz2010] AS [work_taz2010]
		,[prev_work_waValue] AS [prev_work_wa]
		,[prev_work_county] AS [prev_work_county]
		,[prev_work_tract] AS [prev_work_tract]
		,[prev_work_bg] AS [prev_work_bg]
		,[prev_work_puma10] AS [prev_work_puma10]
		,[prev_work_rgcname] AS [prev_work_rgcname]
		,[prev_work_taz2010] AS [prev_work_taz2010]
		,[prev_work_notwa_city] AS [prev_work_notwa_city]
		,[prev_work_notwa_zip] AS [prev_work_notwa_zip]
		,[prev_work_notwa_state] AS [prev_work_notwa_state]
		,[prev_work_notwa_notusValue] AS [prev_work_notwa_notus]
		,[school_freqValue] AS [school_freq]
		,[school_loc_county] AS [school_loc_county]
		,[school_tract] AS [school_tract]
		,[school_bg] AS [school_bg]
		,[school_puma10] AS [school_puma10]
		,[school_rgcname] AS [school_rgcname]
		,[school_taz2010] AS [school_taz2010]
		,[completed_pref_surveyValue] AS [completed_pref_survey]
		,[mode_freq_1Value] AS [mode_freq_1]
		,[mode_freq_2Value] AS [mode_freq_2]
		,[mode_freq_3Value] AS [mode_freq_3]
		,[mode_freq_4Value] AS [mode_freq_4]
		,[mode_freq_5Value] AS [mode_freq_5]
		,[tran_pass_1Value] AS [tran_pass_1]
		,[tran_pass_2Value] AS [tran_pass_2]
		,[tran_pass_3Value] AS [tran_pass_3]
		,[tran_pass_4Value] AS [tran_pass_4]
		,[tran_pass_5Value] AS [tran_pass_5]
		,[tran_pass_6Value] AS [tran_pass_6]
		,[tran_pass_7Value] AS [tran_pass_7]
		,[tran_pass_8Value] AS [tran_pass_8]
		,[tran_pass_9Value] AS [tran_pass_9]
		,[tran_pass_10Value] AS [tran_pass_10]
		,[tran_pass_11Value] AS [tran_pass_11]
		,[tran_pass_12Value] AS [tran_pass_12]
		,[benefits_1Value] AS [benefits_1]
		,[benefits_2Value] AS [benefits_2]
		,[benefits_3Value] AS [benefits_3]
		,[benefits_4Value] AS [benefits_4]
		,[av_interest_1Value] AS [av_interest_1]
		,[av_interest_2Value] AS [av_interest_2]
		,[av_interest_3Value] AS [av_interest_3]
		,[av_interest_4Value] AS [av_interest_4]
		,[av_interest_5Value] AS [av_interest_5]
		,[av_interest_6Value] AS [av_interest_6]
		,[av_interest_7Value] AS [av_interest_7]
		,[av_concern_1Value] AS [av_concern_1]
		,[av_concern_2Value] AS [av_concern_2]
		,[av_concern_3Value] AS [av_concern_3]
		,[av_concern_4Value] AS [av_concern_4]
		,[av_concern_5Value] AS [av_concern_5]
		,[wbt_transitmore_1Value] AS [wbt_transitmore_1]
		,[wbt_transitmore_2Value] AS [wbt_transitmore_2]
		,[wbt_transitmore_3Value] AS [wbt_transitmore_3]
		,[wbt_bikemore_1] AS [wbt_bikemore_1]
		,[wbt_bikemore_2] AS [wbt_bikemore_2]
		,[wbt_bikemore_3] AS [wbt_bikemore_3]
		,[wbt_bikemore_4] AS [wbt_bikemore_4]
		,[wbt_bikemore_5] AS [wbt_bikemore_5]
		,[rmove_incentiveValue] AS [rmove_incentive]
		,[call_centerValue] AS [call_center]
		,[mobile_deviceValue] AS [mobile_device]
		,[num_trips] AS [num_trips]
		,[nwkdaysValue] AS [nwkdays]
		,[hh_wt_revised] AS [hh_wt_revised]
		,[hh_day_wt_revised] AS [hh_day_wt_revised]
		,[studentind] AS [studentind]
		  ,BINARY_CHECKSUM(*) AS DWCHECKSUM
	  FROM [stg].[ResponseAndCodePerson_2017]
	)
	MERGE HHSurvey.PersonDim2017 AS target
	USING cte AS source
	ON
	(
		target.PersonDimID = source.PersonDimID
	)
	WHEN MATCHED AND COALESCE(target.DWCHECKSUM,-1) <> COALESCE(source.DWCHECKSUM, -1) THEN
		UPDATE SET
		target.[hhid] = source.[hhid]
		,target.[personid] = source.[personid]
		,target.[pernum] = source.[pernum]
		,target.[sample_segment] = source.[sample_segment]
		,target.[hhgroup] = source.[hhgroup]
		,target.[traveldate] = source.[traveldate]
		,target.[relationship] = source.[relationship]
		,target.[proxy_parent] = source.[proxy_parent]
		,target.[proxy] = source.[proxy]
		,target.[age] = source.[age]
		,target.[gender] = source.[gender]
		,target.[employment] = source.[employment]
		,target.[jobs_count] = source.[jobs_count]
		,target.[worker] = source.[worker]
		,target.[student] = source.[student]
		,target.[schooltype] = source.[schooltype]
		,target.[education] = source.[education]
		,target.[license] = source.[license]
		,target.[vehicleused] = source.[vehicleused]
		,target.[smartphone_type] = source.[smartphone_type]
		,target.[smartphone_age] = source.[smartphone_age]
		,target.[smartphone_qualified] = source.[smartphone_qualified]
		,target.[race_afam] = source.[race_afam]
		,target.[race_aiak] = source.[race_aiak]
		,target.[race_asian] = source.[race_asian]
		,target.[race_hapi] = source.[race_hapi]
		,target.[race_hisp] = source.[race_hisp]
		,target.[race_white] = source.[race_white]
		,target.[race_other] = source.[race_other]
		,target.[race_noanswer] = source.[race_noanswer]
		,target.[workplace] = source.[workplace]
		,target.[hours_work] = source.[hours_work]
		,target.[commute_freq] = source.[commute_freq]
		,target.[commute_mode] = source.[commute_mode]
		,target.[commute_dur] = source.[commute_dur]
		,target.[telecommute_freq] = source.[telecommute_freq]
		,target.[wpktyp] = source.[wpktyp]
		,target.[workpass] = source.[workpass]
		,target.[workpass_cost] = source.[workpass_cost]
		,target.[workpass_cost_dk] = source.[workpass_cost_dk]
		,target.[work_county] = source.[work_county]
		,target.[work_tract] = source.[work_tract]
		,target.[work_bg] = source.[work_bg]
		,target.[work_puma10] = source.[work_puma10]
		,target.[work_rgcname] = source.[work_rgcname]
		,target.[work_taz2010] = source.[work_taz2010]
		,target.[prev_work_wa] = source.[prev_work_wa]
		,target.[prev_work_county] = source.[prev_work_county]
		,target.[prev_work_tract] = source.[prev_work_tract]
		,target.[prev_work_bg] = source.[prev_work_bg]
		,target.[prev_work_puma10] = source.[prev_work_puma10]
		,target.[prev_work_rgcname] = source.[prev_work_rgcname]
		,target.[prev_work_taz2010] = source.[prev_work_taz2010]
		,target.[prev_work_notwa_city] = source.[prev_work_notwa_city]
		,target.[prev_work_notwa_zip] = source.[prev_work_notwa_zip]
		,target.[prev_work_notwa_state] = source.[prev_work_notwa_state]
		,target.[prev_work_notwa_notus] = source.[prev_work_notwa_notus]
		,target.[school_freq] = source.[school_freq]
		,target.[school_loc_county] = source.[school_loc_county]
		,target.[school_tract] = source.[school_tract]
		,target.[school_bg] = source.[school_bg]
		,target.[school_puma10] = source.[school_puma10]
		,target.[school_rgcname] = source.[school_rgcname]
		,target.[school_taz2010] = source.[school_taz2010]
		,target.[completed_pref_survey] = source.[completed_pref_survey]
		,target.[mode_freq_1] = source.[mode_freq_1]
		,target.[mode_freq_2] = source.[mode_freq_2]
		,target.[mode_freq_3] = source.[mode_freq_3]
		,target.[mode_freq_4] = source.[mode_freq_4]
		,target.[mode_freq_5] = source.[mode_freq_5]
		,target.[tran_pass_1] = source.[tran_pass_1]
		,target.[tran_pass_2] = source.[tran_pass_2]
		,target.[tran_pass_3] = source.[tran_pass_3]
		,target.[tran_pass_4] = source.[tran_pass_4]
		,target.[tran_pass_5] = source.[tran_pass_5]
		,target.[tran_pass_6] = source.[tran_pass_6]
		,target.[tran_pass_7] = source.[tran_pass_7]
		,target.[tran_pass_8] = source.[tran_pass_8]
		,target.[tran_pass_9] = source.[tran_pass_9]
		,target.[tran_pass_10] = source.[tran_pass_10]
		,target.[tran_pass_11] = source.[tran_pass_11]
		,target.[tran_pass_12] = source.[tran_pass_12]
		,target.[benefits_1] = source.[benefits_1]
		,target.[benefits_2] = source.[benefits_2]
		,target.[benefits_3] = source.[benefits_3]
		,target.[benefits_4] = source.[benefits_4]
		,target.[av_interest_1] = source.[av_interest_1]
		,target.[av_interest_2] = source.[av_interest_2]
		,target.[av_interest_3] = source.[av_interest_3]
		,target.[av_interest_4] = source.[av_interest_4]
		,target.[av_interest_5] = source.[av_interest_5]
		,target.[av_interest_6] = source.[av_interest_6]
		,target.[av_interest_7] = source.[av_interest_7]
		,target.[av_concern_1] = source.[av_concern_1]
		,target.[av_concern_2] = source.[av_concern_2]
		,target.[av_concern_3] = source.[av_concern_3]
		,target.[av_concern_4] = source.[av_concern_4]
		,target.[av_concern_5] = source.[av_concern_5]
		,target.[wbt_transitmore_1] = source.[wbt_transitmore_1]
		,target.[wbt_transitmore_2] = source.[wbt_transitmore_2]
		,target.[wbt_transitmore_3] = source.[wbt_transitmore_3]
		,target.[wbt_bikemore_1] = source.[wbt_bikemore_1]
		,target.[wbt_bikemore_2] = source.[wbt_bikemore_2]
		,target.[wbt_bikemore_3] = source.[wbt_bikemore_3]
		,target.[wbt_bikemore_4] = source.[wbt_bikemore_4]
		,target.[wbt_bikemore_5] = source.[wbt_bikemore_5]
		,target.[rmove_incentive] = source.[rmove_incentive]
		,target.[call_center] = source.[call_center]
		,target.[mobile_device] = source.[mobile_device]
		,target.[num_trips] = source.[num_trips]
		,target.[nwkdays] = source.[nwkdays]
		,target.[hh_wt_revised] = source.[hh_wt_revised]
		,target.[hh_day_wt_revised] = source.[hh_day_wt_revised]
		,target.[studentind] = source.[studentind]
	WHEN NOT MATCHED THEN INSERT (
		[PersonDimID]
		,[hhid]
		,[personid]
		,[pernum]
		,[sample_segment]
		,[hhgroup]
		,[traveldate]
		,[relationship]
		,[proxy_parent]
		,[proxy]
		,[age]
		,[gender]
		,[employment]
		,[jobs_count]
		,[worker]
		,[student]
		,[schooltype]
		,[education]
		,[license]
		,[vehicleused]
		,[smartphone_type]
		,[smartphone_age]
		,[smartphone_qualified]
		,[race_afam]
		,[race_aiak]
		,[race_asian]
		,[race_hapi]
		,[race_hisp]
		,[race_white]
		,[race_other]
		,[race_noanswer]
		,[workplace]
		,[hours_work]
		,[commute_freq]
		,[commute_mode]
		,[commute_dur]
		,[telecommute_freq]
		,[wpktyp]
		,[workpass]
		,[workpass_cost]
		,[workpass_cost_dk]
		,[work_county]
		,[work_tract]
		,[work_bg]
		,[work_puma10]
		,[work_rgcname]
		,[work_taz2010]
		,[prev_work_wa]
		,[prev_work_county]
		,[prev_work_tract]
		,[prev_work_bg]
		,[prev_work_puma10]
		,[prev_work_rgcname]
		,[prev_work_taz2010]
		,[prev_work_notwa_city]
		,[prev_work_notwa_zip]
		,[prev_work_notwa_state]
		,[prev_work_notwa_notus]
		,[school_freq]
		,[school_loc_county]
		,[school_tract]
		,[school_bg]
		,[school_puma10]
		,[school_rgcname]
		,[school_taz2010]
		,[completed_pref_survey]
		,[mode_freq_1]
		,[mode_freq_2]
		,[mode_freq_3]
		,[mode_freq_4]
		,[mode_freq_5]
		,[tran_pass_1]
		,[tran_pass_2]
		,[tran_pass_3]
		,[tran_pass_4]
		,[tran_pass_5]
		,[tran_pass_6]
		,[tran_pass_7]
		,[tran_pass_8]
		,[tran_pass_9]
		,[tran_pass_10]
		,[tran_pass_11]
		,[tran_pass_12]
		,[benefits_1]
		,[benefits_2]
		,[benefits_3]
		,[benefits_4]
		,[av_interest_1]
		,[av_interest_2]
		,[av_interest_3]
		,[av_interest_4]
		,[av_interest_5]
		,[av_interest_6]
		,[av_interest_7]
		,[av_concern_1]
		,[av_concern_2]
		,[av_concern_3]
		,[av_concern_4]
		,[av_concern_5]
		,[wbt_transitmore_1]
		,[wbt_transitmore_2]
		,[wbt_transitmore_3]
		,[wbt_bikemore_1]
		,[wbt_bikemore_2]
		,[wbt_bikemore_3]
		,[wbt_bikemore_4]
		,[wbt_bikemore_5]
		,[rmove_incentive]
		,[call_center]
		,[mobile_device]
		,[num_trips]
		,[nwkdays]
		,[hh_wt_revised]
		,[hh_day_wt_revised]
		,[studentind]
	)
	VALUES
	(
		source.[PersonDimID]
		,source.[hhid]
		,source.[personid]
		,source.[pernum]
		,source.[sample_segment]
		,source.[hhgroup]
		,source.[traveldate]
		,source.[relationship]
		,source.[proxy_parent]
		,source.[proxy]
		,source.[age]
		,source.[gender]
		,source.[employment]
		,source.[jobs_count]
		,source.[worker]
		,source.[student]
		,source.[schooltype]
		,source.[education]
		,source.[license]
		,source.[vehicleused]
		,source.[smartphone_type]
		,source.[smartphone_age]
		,source.[smartphone_qualified]
		,source.[race_afam]
		,source.[race_aiak]
		,source.[race_asian]
		,source.[race_hapi]
		,source.[race_hisp]
		,source.[race_white]
		,source.[race_other]
		,source.[race_noanswer]
		,source.[workplace]
		,source.[hours_work]
		,source.[commute_freq]
		,source.[commute_mode]
		,source.[commute_dur]
		,source.[telecommute_freq]
		,source.[wpktyp]
		,source.[workpass]
		,source.[workpass_cost]
		,source.[workpass_cost_dk]
		,source.[work_county]
		,source.[work_tract]
		,source.[work_bg]
		,source.[work_puma10]
		,source.[work_rgcname]
		,source.[work_taz2010]
		,source.[prev_work_wa]
		,source.[prev_work_county]
		,source.[prev_work_tract]
		,source.[prev_work_bg]
		,source.[prev_work_puma10]
		,source.[prev_work_rgcname]
		,source.[prev_work_taz2010]
		,source.[prev_work_notwa_city]
		,source.[prev_work_notwa_zip]
		,source.[prev_work_notwa_state]
		,source.[prev_work_notwa_notus]
		,source.[school_freq]
		,source.[school_loc_county]
		,source.[school_tract]
		,source.[school_bg]
		,source.[school_puma10]
		,source.[school_rgcname]
		,source.[school_taz2010]
		,source.[completed_pref_survey]
		,source.[mode_freq_1]
		,source.[mode_freq_2]
		,source.[mode_freq_3]
		,source.[mode_freq_4]
		,source.[mode_freq_5]
		,source.[tran_pass_1]
		,source.[tran_pass_2]
		,source.[tran_pass_3]
		,source.[tran_pass_4]
		,source.[tran_pass_5]
		,source.[tran_pass_6]
		,source.[tran_pass_7]
		,source.[tran_pass_8]
		,source.[tran_pass_9]
		,source.[tran_pass_10]
		,source.[tran_pass_11]
		,source.[tran_pass_12]
		,source.[benefits_1]
		,source.[benefits_2]
		,source.[benefits_3]
		,source.[benefits_4]
		,source.[av_interest_1]
		,source.[av_interest_2]
		,source.[av_interest_3]
		,source.[av_interest_4]
		,source.[av_interest_5]
		,source.[av_interest_6]
		,source.[av_interest_7]
		,source.[av_concern_1]
		,source.[av_concern_2]
		,source.[av_concern_3]
		,source.[av_concern_4]
		,source.[av_concern_5]
		,source.[wbt_transitmore_1]
		,source.[wbt_transitmore_2]
		,source.[wbt_transitmore_3]
		,source.[wbt_bikemore_1]
		,source.[wbt_bikemore_2]
		,source.[wbt_bikemore_3]
		,source.[wbt_bikemore_4]
		,source.[wbt_bikemore_5]
		,source.[rmove_incentive]
		,source.[call_center]
		,source.[mobile_device]
		,source.[num_trips]
		,source.[nwkdays]
		,source.[hh_wt_revised]
		,source.[hh_day_wt_revised]
		,source.[studentind]
	);

END
GO

