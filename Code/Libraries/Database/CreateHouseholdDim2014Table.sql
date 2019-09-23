drop table if exists HHSurvey.HouseholdDim2014
create table HHSurvey.HouseholdDim2014 (
	[HouseholdDimID] [BIGINT]
	,[hhid] [BIGINT]
	,[traveldate] [DATETIME2]
	,[dayofweek] [VARCHAR](20)
	,[hhnumtrips] [SMALLINT]
	,[vehicle_count] [VARCHAR](100)
	,[hhsize] [VARCHAR](100)
	,[numadults] [VARCHAR](20)
	,[numchildren] [VARCHAR](100)
	,[numworkers] [TINYINT]
	,[lifecycle] [VARCHAR](100)
	,[hh_income_detailed] [VARCHAR](100)
	,[hh_income_followup] [VARCHAR](100)
	,[hh_income_broad] [VARCHAR](100)
	,[hh_income_detailed_imp] [VARCHAR](100)
	,[hh_income_samp_est] [VARCHAR](100)
	,[sample_segname] [VARCHAR](100)
	,[h_cnty] [VARCHAR](100)
	,[h_city] [VARCHAR](100)
	,[h_zip] [INT]
	,[address_use_flag] [VARCHAR](100)
	,[h_segname] [VARCHAR](100)
	,[h_segname_wgt] [VARCHAR](100)
	,[h_county_name] [VARCHAR](20)
	,[h_district_name] [VARCHAR](100)
	,[h_rgc_name] [VARCHAR](100)
	,[h_school_district_name] [VARCHAR](100)
	,[h_uv_name] [VARCHAR](100)
	,[h_uv_group] [VARCHAR](100)
	,[h_tract] [FLOAT]
	,[h_bg] [FLOAT]
	,[h_puma12] [VARCHAR](100)
	,[res_months] [VARCHAR](100)
	,[res_dur] [VARCHAR](100)
	,[rent_own] [VARCHAR](100)
	,[res_type] [VARCHAR](100)
	,[res_factors_hhchange] [VARCHAR](100)
	,[res_factors_afford] [VARCHAR](100)
	,[res_factors_school] [VARCHAR](100)
	,[res_factors_walk] [VARCHAR](100)
	,[res_factors_space] [VARCHAR](100)
	,[res_factors_closefam] [VARCHAR](100)
	,[res_factors_transit] [VARCHAR](100)
	,[res_factors_hwy] [VARCHAR](100)
	,[res_factors_30min] [VARCHAR](100)
	,[prev_rent_own] [VARCHAR](100)
	,[prev_res_type] [VARCHAR](100)
	,[prev_home_wa] [VARCHAR](20)
	,[prev_home_loc_cnty] [VARCHAR](100)
	,[prev_home_loc_city] [VARCHAR](100)
	,[prev_home_loc_zip] [INT]
	,[prev_home_loc_st] [VARCHAR](20)
	,[prev_tract] [FLOAT]
	,[prev_bg] [FLOAT]
	,[prev_puma12] [FLOAT]
	,[prev_rgc_name] [VARCHAR](100)
	,[prev_home_loc_x] [VARCHAR](20)
	,[cityofseattle] [VARCHAR](100)
	,[expwt_2] [FLOAT]
	,[dataset] [VARCHAR](100)
	,[DWCHECKSUM] BIGINT
 CONSTRAINT [PK_HouseholdDim2014] PRIMARY KEY CLUSTERED (HouseholdDimID)
)