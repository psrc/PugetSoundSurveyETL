/****** Object:  StoredProcedure [dbo].[mergePersonDim2015]    Script Date: 8/12/2019 9:57:52 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROC [dbo].[mergePersonDim2015] 
AS
BEGIN


-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
--	NAME        : [dbo].[mergePersonDim2015] 
--	CREATED BY  : William Andrus
--	DATE        : 2019-07-29
--	DESCRIPTION : Merge Person Dim Table for 2015 
--	NOTES       : none
--	USAGE		: exec [dbo].[mergePersonDim2015] 
-----------------------------------------------------------------------------------------------------------------------
--	VER:    DATE:       NAME:           DESCRIPTION OF CHANGE:
--	------- ----------- --------------- -------------------------------------------------------------------------------
--	1.0     2019.07.29	William Andrus	Initial Creation
-----------------------------------------------------------------------------------------------------------------------

;with cte as 
(
	  SELECT 
	  cast([personid] as int) as PersonDimID
	  ,[hhid]
      ,cast([personid] as char(10)) as [personid]
      ,cast([pernum] as tinyint) [pernum]
	  ,resptypeValue as resptype  
      ,ageValue as [agegroup]
	  ,relationshipValue as [relationship]
	  ,genderValue as [gender]		
	  ,employmentValue as	[employment]
	  ,jobs_countValue as	[jobs_count]
	  ,workerValue as [workerind]
	  ,studentind	
	  ,schoolValue as [school]	
	  ,educationValue as [education]
	  ,smartphoneValue as	[smartphone]
	  ,licenseValue as [license]		
      ,vehicleValue as	[vehicle]
      ,tollfreqValue as [tollfreq]
      ,work_cntyValue as [work_cnty]
      ,work_city as [work_city]
      ,work_zip as [work_zip]
      ,work_st as [work_st]
      ,work_tract as [work_tract]
      ,work_bg as [work_bg]
      ,work_taz2010 as [work_taz2010]
      ,work_puma12 as [work_puma12]
      ,work_rgc_name as [work_rgc_name]
      ,school_freqValue as [school_freq]
      ,school_cntyValue as [school_cnty]
      ,school_city as [school_city]
      ,school_zip as [school_zip]
      ,school_st as [school_st]
      ,school_tract as [school_tract]
      ,school_bg as [school_bg]
      ,school_taz2010 as [school_taz2010]
      ,school_puma12 as [school_puma12]
      ,school_rgc_name as [school_rgc_name]
      ,trips_yesnoValue as [trips_yesno]
      ,numtrips as [numtrips]
      ,loc_startValue as [loc_start]
      ,loc_endValue as [loc_end]
      ,no_travelValue as [no_travel]
      ,noneed_dayoffValue as [noneed_dayoff]
      ,noneed_telecommuteValue as [noneed_telecommute]
      ,noneed_workhomenoapayValue as [noneed_workhomenoapay]
      ,noneed_kidsvacationValue as [noneed_kidsvacation]
      ,noneed_otherValue as [noneed_other]
      ,unable_notransportValue as [unable_notransport]
      ,unable_sickValue as [unable_sick]
      ,unable_visitorValue as [unable_visitor]
      ,unable_otherValue as [unable_other]
      ,added_trip_flagValue as [added_trip_flag]
      ,added_loopValue as [added_loop]
      ,added_quickValue as [added_quick]
      ,added_stopValue as [added_stop]
      ,added_dropoffValue as [added_dropoff]
      ,added_parkingValue as [added_parking]
      ,added_otherValue as [added_other]
      ,typicalValue as [typical]
      ,purchaseValue as [purchase]
      ,transit_freqValue as [transit_freq]
      ,bike_freqValue as [bike_freq]
      ,walk_freqValue as [walk_freq]
      ,transitpay_orcaValue as [transitpay_orca]
      ,transitpay_cashValue as [transitpay_cash]
      ,transitpay_ticketsValue as [transitpay_tickets]
      ,transitpay_upassValue as [transitpay_upass]
      ,transitpay_permitValue as [transitpay_permit]
      ,transitpay_flexValue as [transitpay_flex]
      ,transitpay_accessValue as [transitpay_access]
      ,transitpay_schoolValue as [transitpay_school]
      ,transitpay_govtValue as [transitpay_govt]
      ,transitpay_otherValue as [transitpay_other]
      ,transitpay_dontknowValue as [transitpay_dontknow]
      ,transitpay_freeValue as [transitpay_free]
      ,transit_subsidyValue as [transit_subsidy]
      ,hours_workValue as [hours_work]
      ,commute_freqValue as [commute_freq]
      ,telecommute_freqValue as [telecommute_freq]
      ,commute_durValue as [commute_dur]
      ,selfemp_durValue as [selfemp_dur]
      ,drive_livingValue as [drive_living]
      ,night_shiftValue as [night_shift]
      ,choose_workValue as [choose_work]
      ,choose_weatherValue as [choose_weather]
      ,telecommuteValue as [telecommute]
      ,telecommute_hours as [telecommute_hours]
      ,commute_modeValue as [commute_mode]
      ,benefits_flextimeValue as [benefits_flextime]
      ,benefits_compressedValue as [benefits_compressed]
      ,benefits_parkingValue as [benefits_parking]
      ,benefits_transitValue as [benefits_transit]
      ,benefits_commuteValue as [benefits_commute]
      ,wpktypValue as [wpktyp]
      ,wpk_dur as [wpk_dur]
      ,wpk_dis as [wpk_dis]
      ,prev_work_waValue as [prev_work_wa]
      ,prev_work_loc_cntyValue as [prev_work_loc_cnty]
      ,prev_work_loc_city as [prev_work_loc_city]
      ,prev_work_loc_zip as [prev_work_loc_zip]
      ,prev_work_loc_st as [prev_work_loc_st]
      ,prev_work_tract as [prev_work_tract]
      ,prev_work_bg as [prev_work_bg]
      ,prev_work_taz2010 as [prev_work_taz2010]
      ,prev_work_puma12 as [prev_work_puma12]
      ,prev_work_rgc_name as [prev_work_rgc_name]
      ,prev_work_loc_xValue as [prev_work_loc_x]
      ,smartphone_typeValue as [smartphone_type]
      ,smartphone_type6_r as [smartphone_type6_r]
      ,smartphone_drValue as [smartphone_dr]
      ,smartphone_dr14_r as [smartphone_dr14_r]
      ,smartphone_ipValue as [smartphone_ip]
      ,smartphone_ip9_r as [smartphone_ip9_r]
      ,carpool_gascostValue as [carpool_gascost]
      ,carpool_parkingcostValue as [carpool_parkingcost]
      ,carpool_tollsValue as [carpool_tolls]
      ,carpool_hovValue as [carpool_hov]
      ,transit_availValue as [transit_avail]
      ,carpool_otherValue as [carpool_other]
      ,carpool_noneValue as [carpool_none]
      ,carpool_naValue as [carpool_na]
      ,carshare_car2goValue as [carshare_car2go]
      ,carshare_relayridesValue as [carshare_relayrides]
      ,carshare_zipcarValue as [carshare_zipcar]
      ,carshare_noneValue as [carshare_none]
      ,carshare_dontknowValue as [carshare_dontknow]
      ,carshare_otherValue as [carshare_other]
      ,prontoshareValue as [prontoshare]
      ,share_freq_car2goValue as [share_freq_car2go]
      ,share_freq_relayridesValue as [share_freq_relayrides]
      ,share_freq_zipcarValue as [share_freq_zipcar]
      ,share_freq_othercarValue as [share_freq_othercar]
      ,share_freq_lyftValue as [share_freq_lyft]
      ,share_freq_sidecarValue as [share_freq_sidecar]
      ,share_freq_uberxValue as [share_freq_uberx]
      ,share_freq_prontoValue as [share_freq_pronto]
      ,av_interest_nodriverValue as [av_interest_nodriver]
      ,av_interest_backupdriverValue as [av_interest_backupdriver]
      ,av_interest_commutesovValue as [av_interest_commutesov]
      ,av_interest_commutehovValue as [av_interest_commutehov]
      ,av_interest_ownValue as [av_interest_own]
      ,av_interest_carshareValue as [av_interest_carshare]
      ,av_interest_shortValue as [av_interest_short]
      ,av_concern_safeequipValue as [av_concern_safeequip]
      ,av_concern_legalValue as [av_concern_legal]
      ,av_concern_safevehValue as [av_concern_safeveh]
      ,av_concern_reactValue as [av_concern_react]
      ,av_concern_performValue as [av_concern_perform]
      ,wbt_more_transitsafetyValue as [wbt_more_transitsafety]
      ,wbt_more_transitfreqValue as [wbt_more_transitfreq]
      ,wbt_more_reliabilityValue as [wbt_more_reliability]
      ,bike3mileValue as [bike3mile]
      ,telecommute_ifValue as [telecommute_if]
      ,diary_duration_minutes as [diary_duration_minutes]
      ,user_ismobiledevice as [user_ismobiledevice]
      ,person_should_take_diaryValue as [person_should_take_diary]
      ,proxyValue as [proxy]
      ,expwt_h1415 as [expwt_h1415]
	  ,BINARY_CHECKSUM(*) as DWCHECKSUM
  FROM [stg].[ResponseAndCode_2015] person
)

MERGE dbo.PersonDimTmp AS target  
USING cte AS source
ON 
(
	target.personid = source.personid
)  
WHEN MATCHED AND coalesce(target.DWCHECKSUM,-1) <> coalesce(source.DWCHECKSUM,-1) THEN
	UPDATE SET
      target.[pernum] = source.pernum 
      ,target.[resptype] = source.resptype 
      ,target.[agegroup] = source.agegroup 
      ,target.[relationship] = source.relationship 
      ,target.[gender] = source.gender
      ,target.[employment] = source.employment
	  ,target.[jobs_count] = source.jobs_count 
      ,target.[workerind] = source.workerind 
      ,target.[studentind] = source.studentind
      ,target.[school] = source.school
      ,target.[education] = source.education
      ,target.[smartphone] = source.smartphone 
      ,target.[license] = source.license
      ,target.[vehicle] = source.vehicle
      ,target.[tollfreq] = source.tollfreq
      ,target.[work_cnty] = source.work_cnty 
      ,target.[work_city] = source.work_city 
      ,target.[work_zip] = source.work_zip 
      ,target.[work_st] = source.work_st 
      ,target.[work_tract] = source.work_tract 
      ,target.[work_bg] = source.work_bg 
      ,target.[work_taz2010] = source.work_taz2010 
      ,target.[work_puma12] = source.work_puma12 
      ,target.[work_rgc_name] = source.work_rgc_name 
      ,target.[school_freq] = source.school_freq 
      ,target.[school_cnty] = source.school_cnty 
      ,target.[school_city] = source.school_city 
      ,target.[school_zip] = source.school_zip 
      ,target.[school_st] = source.school_st 
      ,target.[school_tract] = source.school_tract 
      ,target.[school_bg] = source.school_bg 
      ,target.[school_taz2010] = source.school_taz2010 
      ,target.[school_puma12] = source.school_puma12 
      ,target.[school_rgc_name] = source.school_rgc_name 
      ,target.[trips_yesno] = source.trips_yesno
      ,target.[loc_start] = source.loc_start
      ,target.[loc_end] = source.loc_end
      ,target.[no_travel] = source.no_travel
      ,target.[noneed_dayoff] = source.noneed_dayoff
      ,target.[noneed_telecommute] = source.noneed_telecommute
      ,target.[noneed_workhomenoapay] = source.noneed_workhomenoapay
      ,target.[noneed_kidsvacation] = source.noneed_kidsvacation
      ,target.[noneed_other] = source.noneed_other
      ,target.[unable_notransport] = source.unable_notransport
      ,target.[unable_sick] = source.unable_sick
      ,target.[unable_visitor] = source.unable_visitor
      ,target.[unable_other] = source.unable_other
      ,target.[added_trip_flag] = source.added_trip_flag
      ,target.[added_loop] = source.added_loop
      ,target.[added_quick] = source.added_quick
      ,target.[added_stop] = source.added_stop
      ,target.[added_dropoff] = source.added_dropoff
      ,target.[added_parking] = source.added_parking
      ,target.[added_other] = source.added_other
      ,target.[typical] = source.typical
      ,target.[purchase] = source.purchase
      ,target.[transit_freq] = source.transit_freq
      ,target.[bike_freq] = source.bike_freq
      ,target.[walk_freq] = source.walk_freq
      ,target.[transitpay_orca] = source.transitpay_orca
      ,target.[transitpay_cash] = source.transitpay_cash
      ,target.[transitpay_tickets] = source.transitpay_tickets
      ,target.[transitpay_upass] = source.transitpay_upass
      ,target.[transitpay_permit] = source.transitpay_permit
      ,target.[transitpay_flex] = source.transitpay_flex
      ,target.[transitpay_access] = source.transitpay_access
      ,target.[transitpay_school] = source.transitpay_school
      ,target.[transitpay_govt] = source.transitpay_govt
      ,target.[transitpay_other] = source.transitpay_other
      ,target.[transitpay_dontknow] = source.transitpay_dontknow
      ,target.[transitpay_free] = source.transitpay_free
      ,target.[transit_subsidy] = source.transit_subsidy
      ,target.[hours_work] = source.hours_work
      ,target.[commute_freq] = source.commute_freq
      ,target.[telecommute_freq] = source.telecommute_freq
      ,target.[commute_dur] = source.commute_dur
      ,target.[selfemp_dur] = source.selfemp_dur
      ,target.[drive_living] = source.drive_living
      ,target.[night_shift] = source.night_shift
      ,target.[choose_work] = source.choose_work
      ,target.[choose_weather] = source.choose_weather
      ,target.[telecommute] = source.telecommute
      ,target.[telecommute_hours] = source.telecommute_hours
      ,target.[commute_mode] = source.commute_mode
      ,target.[benefits_flextime] = source.benefits_flextime
      ,target.[benefits_compressed] = source.benefits_compressed
      ,target.[benefits_parking] = source.benefits_parking
      ,target.[benefits_transit] = source.benefits_transit
      ,target.[benefits_commute] = source.benefits_commute
      ,target.[wpktyp] = source.wpktyp
      ,target.[wpk_dur] = source.wpk_dur
      ,target.[wpk_dis] = source.wpk_dis
      ,target.[prev_work_wa] = source.prev_work_wa
      ,target.[prev_work_loc_cnty] = source.prev_work_loc_cnty
      ,target.[prev_work_loc_city] = source.prev_work_loc_city
      ,target.[prev_work_loc_zip] = source.prev_work_loc_zip
      ,target.[prev_work_loc_st] = source.prev_work_loc_st
      ,target.[prev_work_tract] = source.prev_work_tract
      ,target.[prev_work_bg] = source.prev_work_bg
      ,target.[prev_work_taz2010] = source.prev_work_taz2010
      ,target.[prev_work_puma12] = source.prev_work_puma12
      ,target.[prev_work_rgc_name] = source.prev_work_rgc_name
      ,target.[prev_work_loc_x] = source.prev_work_loc_x
      ,target.[smartphone_type] = source.smartphone_type
      ,target.[smartphone_type6_r] = source.smartphone_type6_r
      ,target.[smartphone_dr] = source.smartphone_dr
      ,target.[smartphone_dr14_r] = source.smartphone_dr14_r
      ,target.[smartphone_ip] = source.smartphone_ip
      ,target.[smartphone_ip9_r] = source.smartphone_ip9_r
      ,target.[carpool_gascost] = source.carpool_gascost
      ,target.[carpool_parkingcost] = source.carpool_parkingcost
      ,target.[carpool_tolls] = source.carpool_tolls
      ,target.[carpool_hov] = source.carpool_hov
      ,target.[transit_avail] = source.transit_avail
      ,target.[carpool_other] = source.carpool_other
      ,target.[carpool_none] = source.carpool_none
      ,target.[carpool_na] = source.carpool_na
      ,target.[carshare_car2go] = source.carshare_car2go
      ,target.[carshare_relayrides] = source.carshare_relayrides
      ,target.[carshare_zipcar] = source.carshare_zipcar
      ,target.[carshare_none] = source.carshare_none
      ,target.[carshare_dontknow] = source.carshare_dontknow
      ,target.[carshare_other] = source.carshare_other
      ,target.[prontoshare] = source.prontoshare
      ,target.[share_freq_car2go] = source.share_freq_car2go
      ,target.[share_freq_relayrides] = source.share_freq_relayrides
      ,target.[share_freq_zipcar] = source.share_freq_zipcar
      ,target.[share_freq_othercar] = source.share_freq_othercar
      ,target.[share_freq_lyft] = source.share_freq_lyft
      ,target.[share_freq_sidecar] = source.share_freq_sidecar
      ,target.[share_freq_uberx] = source.share_freq_uberx
      ,target.[share_freq_pronto] = source.share_freq_pronto
      ,target.[av_interest_nodriver] = source.av_interest_nodriver
      ,target.[av_interest_backupdriver] = source.av_interest_backupdriver
      ,target.[av_interest_commutesov] = source.av_interest_commutesov
      ,target.[av_interest_commutehov] = source.av_interest_commutehov
      ,target.[av_interest_own] = source.av_interest_own
      ,target.[av_interest_carshare] = source.av_interest_carshare
      ,target.[av_interest_short] = source.av_interest_short
      ,target.[av_concern_safeequip] = source.av_concern_safeequip
      ,target.[av_concern_legal] = source.av_concern_legal
      ,target.[av_concern_safeveh] = source.av_concern_safeveh
      ,target.[av_concern_react] = source.av_concern_react
      ,target.[av_concern_perform] = source.av_concern_perform
      ,target.[wbt_more_transitsafety] = source.wbt_more_transitsafety
      ,target.[wbt_more_transitfreq] = source.wbt_more_transitfreq
      ,target.[wbt_more_reliability] = source.wbt_more_reliability
      ,target.[bike3mile] = source.bike3mile
      ,target.[telecommute_if] = source.telecommute_if
      ,target.[user_ismobiledevice] = source.user_ismobiledevice
      ,target.[person_should_take_diary] = source.person_should_take_diary
      ,target.[proxy] = source.proxy
      ,target.[expwt_h1415] = source.expwt_h1415 
      ,target.[DWCHECKSUM] = source.DWCHECKSUM
WHEN NOT MATCHED THEN 
	INSERT 
    (
		PersonDimID
		,[personid]
        ,[pernum]
        ,[resptype]
        ,[agegroup]
        ,[relationship]
        ,[gender]
        ,[employment]
        ,[jobs_count]
        ,[workerind]
        ,[studentind]
        ,[school]
        ,[education]
        ,[smartphone]
        ,[license]
        ,[vehicle]
        ,[tollfreq]
        ,[work_cnty]
        ,[work_city]
        ,[work_zip]
        ,[work_st]
        ,[work_tract]
        ,[work_bg]
        ,[work_taz2010]
        ,[work_puma12]
        ,[work_rgc_name]
        ,[school_freq]
        ,[school_cnty]
        ,[school_city]
        ,[school_zip]
        ,[school_st]
        ,[school_tract]
        ,[school_bg]
        ,[school_taz2010]
        ,[school_puma12]
        ,[school_rgc_name]
        ,[trips_yesno]
        ,[loc_start]
        ,[loc_end]
        ,[no_travel]
        ,[noneed_dayoff]
        ,[noneed_telecommute]
        ,[noneed_workhomenoapay]
        ,[noneed_kidsvacation]
        ,[noneed_other]
        ,[unable_notransport]
        ,[unable_sick]
        ,[unable_visitor]
        ,[unable_other]
        ,[added_trip_flag]
        ,[added_loop]
        ,[added_quick]
        ,[added_stop]
        ,[added_dropoff]
        ,[added_parking]
        ,[added_other]
        ,[typical]
        ,[purchase]
        ,[transit_freq]
        ,[bike_freq]
        ,[walk_freq]
        ,[transitpay_orca]
        ,[transitpay_cash]
        ,[transitpay_tickets]
        ,[transitpay_upass]
        ,[transitpay_permit]
        ,[transitpay_flex]
        ,[transitpay_access]
        ,[transitpay_school]
        ,[transitpay_govt]
        ,[transitpay_other]
        ,[transitpay_dontknow]
        ,[transitpay_free]
        ,[transit_subsidy]
        ,[hours_work]
        ,[commute_freq]
        ,[telecommute_freq]
        ,[commute_dur]
        ,[selfemp_dur]
        ,[drive_living]
        ,[night_shift]
        ,[choose_work]
        ,[choose_weather]
        ,[telecommute]
        ,[telecommute_hours]
        ,[commute_mode]
        ,[benefits_flextime]
        ,[benefits_compressed]
        ,[benefits_parking]
        ,[benefits_transit]
        ,[benefits_commute]
        ,[wpktyp]
        ,[wpk_dur]
        ,[wpk_dis]
        ,[prev_work_wa]
        ,[prev_work_loc_cnty]
        ,[prev_work_loc_city]
        ,[prev_work_loc_zip]
        ,[prev_work_loc_st]
        ,[prev_work_tract]
        ,[prev_work_bg]
        ,[prev_work_taz2010]
        ,[prev_work_puma12]
        ,[prev_work_rgc_name]
        ,[prev_work_loc_x]
        ,[smartphone_type]
        ,[smartphone_type6_r]
        ,[smartphone_dr]
        ,[smartphone_dr14_r]
        ,[smartphone_ip]
        ,[smartphone_ip9_r]
        ,[carpool_gascost]
        ,[carpool_parkingcost]
        ,[carpool_tolls]
        ,[carpool_hov]
        ,[transit_avail]
        ,[carpool_other]
        ,[carpool_none]
        ,[carpool_na]
        ,[carshare_car2go]
        ,[carshare_relayrides]
        ,[carshare_zipcar]
        ,[carshare_none]
        ,[carshare_dontknow]
        ,[carshare_other]
        ,[prontoshare]
        ,[share_freq_car2go]
        ,[share_freq_relayrides]
        ,[share_freq_zipcar]
        ,[share_freq_othercar]
        ,[share_freq_lyft]
        ,[share_freq_sidecar]
        ,[share_freq_uberx]
        ,[share_freq_pronto]
        ,[av_interest_nodriver]
        ,[av_interest_backupdriver]
        ,[av_interest_commutesov]
        ,[av_interest_commutehov]
        ,[av_interest_own]
        ,[av_interest_carshare]
        ,[av_interest_short]
        ,[av_concern_safeequip]
        ,[av_concern_legal]
        ,[av_concern_safeveh]
        ,[av_concern_react]
        ,[av_concern_perform]
        ,[wbt_more_transitsafety]
        ,[wbt_more_transitfreq]
        ,[wbt_more_reliability]
        ,[bike3mile]
        ,[telecommute_if]
        ,[user_ismobiledevice]
        ,[person_should_take_diary]
        ,[proxy]
        ,[expwt_h1415]
        ,[DWCHECKSUM]
	)
     VALUES
     (
	   source.PersonDimID
       ,source.personid 
       ,source.pernum 
       ,source.resptype 
       ,source.agegroup 
       ,source.relationship 
       ,source.gender 
       ,source.employment 
       ,source.jobs_count 
       ,source.workerind 
       ,source.studentind 
       ,source.school 
       ,source.education 
       ,source.smartphone 
       ,source.license 
       ,source.vehicle
       ,source.tollfreq 
       ,source.work_cnty 
       ,source.work_city 
       ,source.work_zip 
       ,source.work_st 
       ,source.work_tract 
       ,source.work_bg 
       ,source.work_taz2010 
       ,source.work_puma12 
       ,source.work_rgc_name 
       ,source.school_freq 
       ,source.school_cnty 
       ,source.school_city 
       ,source.school_zip 
       ,source.school_st 
       ,source.school_tract 
       ,source.school_bg 
       ,source.school_taz2010 
       ,source.school_puma12 
       ,source.school_rgc_name 
       ,source.trips_yesno
       ,source.loc_start
       ,source.loc_end
       ,source.no_travel
       ,source.noneed_dayoff
       ,source.noneed_telecommute
       ,source.noneed_workhomenoapay
       ,source.noneed_kidsvacation
       ,source.noneed_other
       ,source.unable_notransport
       ,source.unable_sick
       ,source.unable_visitor
       ,source.unable_other
       ,source.added_trip_flag
       ,source.added_loop
       ,source.added_quick
       ,source.added_stop
       ,source.added_dropoff
       ,source.added_parking
       ,source.added_other
       ,source.typical
       ,source.purchase
       ,source.transit_freq
       ,source.bike_freq
       ,source.walk_freq
       ,source.transitpay_orca
       ,source.transitpay_cash
       ,source.transitpay_tickets
       ,source.transitpay_upass
       ,source.transitpay_permit
       ,source.transitpay_flex
       ,source.transitpay_access
       ,source.transitpay_school
       ,source.transitpay_govt
       ,source.transitpay_other
       ,source.transitpay_dontknow
       ,source.transitpay_free
       ,source.transit_subsidy
       ,source.hours_work
       ,source.commute_freq
       ,source.telecommute_freq
       ,source.commute_dur
       ,source.selfemp_dur
       ,source.drive_living
       ,source.night_shift
       ,source.choose_work
       ,source.choose_weather
       ,source.telecommute
       ,source.telecommute_hours
       ,source.commute_mode
       ,source.benefits_flextime
       ,source.benefits_compressed
       ,source.benefits_parking
       ,source.benefits_transit
       ,source.benefits_commute
       ,source.wpktyp
       ,source.wpk_dur
       ,source.wpk_dis
       ,source.prev_work_wa
       ,source.prev_work_loc_cnty
       ,source.prev_work_loc_city
       ,source.prev_work_loc_zip
       ,source.prev_work_loc_st
       ,source.prev_work_tract
       ,source.prev_work_bg
       ,source.prev_work_taz2010
       ,source.prev_work_puma12
       ,source.prev_work_rgc_name
       ,source.prev_work_loc_x
       ,source.smartphone_type
       ,source.smartphone_type6_r
       ,source.smartphone_dr
       ,source.smartphone_dr14_r
       ,source.smartphone_ip
       ,source.smartphone_ip9_r
       ,source.carpool_gascost
       ,source.carpool_parkingcost
       ,source.carpool_tolls
       ,source.carpool_hov
       ,source.transit_avail
       ,source.carpool_other
       ,source.carpool_none
       ,source.carpool_na
       ,source.carshare_car2go
       ,source.carshare_relayrides
       ,source.carshare_zipcar
       ,source.carshare_none
       ,source.carshare_dontknow
       ,source.carshare_other
       ,source.prontoshare
       ,source.share_freq_car2go
       ,source.share_freq_relayrides
       ,source.share_freq_zipcar
       ,source.share_freq_othercar
       ,source.share_freq_lyft
       ,source.share_freq_sidecar
       ,source.share_freq_uberx
       ,source.share_freq_pronto
       ,source.av_interest_nodriver
       ,source.av_interest_backupdriver
       ,source.av_interest_commutesov
       ,source.av_interest_commutehov
       ,source.av_interest_own
       ,source.av_interest_carshare
       ,source.av_interest_short
       ,source.av_concern_safeequip
       ,source.av_concern_legal
       ,source.av_concern_safeveh
       ,source.av_concern_react
       ,source.av_concern_perform
       ,source.wbt_more_transitsafety
       ,source.wbt_more_transitfreq
       ,source.wbt_more_reliability
       ,source.bike3mile
       ,source.telecommute_if
       ,source.user_ismobiledevice
       ,source.person_should_take_diary
       ,source.proxy
       ,source.expwt_h1415
       ,source.DWCHECKSUM
	  );


END


GO

