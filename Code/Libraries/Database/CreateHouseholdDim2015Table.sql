USE [Elmer]
GO

/****** Object:  Table [HHSurvey].[HouseholdDim2015]    Script Date: 9/16/2019 1:17:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
drop table if exists HHSurvey.HouseholdDim2015

CREATE TABLE [HHSurvey].[HouseholdDim2015](
	[HouseholdDimID] [bigint] NOT NULL,
	[hhid] [bigint] NULL,
	[sampletype] [nvarchar](100) NULL,
	[tacoma_addon] [nvarchar](100) NULL,
	[traveldate] [datetime2](7) NULL,
	[dayofweek] [varchar](12) NULL,
	[hhnumtrips] [smallint] NULL,
	[vehicle_count] [varchar](50) NULL,
	[hhsize] [varchar](50) NULL,
	[numadults] [varchar](2) NULL,
	[numchildren] [varchar](50) NULL,
	[numworkers] [tinyint] NULL,
	[lifecycle] [varchar](100) NULL,
	[hh_income_detailed] [varchar](100) NULL,
	[hh_income_followup] [varchar](100) NULL,
	[hh_income_broad] [varchar](100) NULL,
	[sample_segname] [varchar](100) NULL,
	[h_cnty] [varchar](100) NULL,
	[h_city] [varchar](100) NULL,
	[h_zip] [int] NULL,
	[address_use_flag] [varchar](200) NULL,
	[panel_moveinspect] [tinyint] NULL,
	[h_segname] [varchar](100) NULL,
	[h_county_name] [varchar](100) NULL,
	[h_district_name] [varchar](100) NULL,
	[h_rgc_name] [varchar](100) NULL,
	[h_school_district_name] [varchar](100) NULL,
	[h_uv_name] [varchar](100) NULL,
	[h_uv_group] [varchar](100) NULL,
	[h_tract] [bigint] NULL,
	[h_bg] [bigint] NULL,
	[h_block] [bigint] NULL,
	[h_taz2010] [int] NULL,
	[h_puma12] [varchar](100) NULL,
	[res_months] [varchar](100) NULL,
	[res_dur] [varchar](100) NULL,
	[rent_own] [varchar](100) NULL,
	[res_type] [varchar](100) NULL,
	[res_factors_hhchange] [varchar](100) NULL,
	[res_factors_afford] [varchar](100) NULL,
	[res_factors_school] [varchar](100) NULL,
	[res_factors_walk] [varchar](100) NULL,
	[res_factors_space] [varchar](100) NULL,
	[res_factors_closefam] [varchar](100) NULL,
	[res_factors_transit] [varchar](100) NULL,
	[res_factors_hwy] [varchar](100) NULL,
	[res_factors_30min] [varchar](100) NULL,
	[prev_rent_own] [varchar](100) NULL,
	[prev_res_type] [varchar](100) NULL,
	[prev_home_wa] [varchar](100) NULL,
	[prev_home_loc_cnty] [varchar](100) NULL,
	[prev_home_loc_city] [varchar](50) NULL,
	[prev_home_loc_zip] [int] NULL,
	[prev_home_loc_st] [varchar](20) NULL,
	[prev_home_taz2010] [int] NULL,
	[prev_home_loc_x] [varchar](100) NULL,
	[hh_info_dur] [int] NULL,
	[incentive_type] [varchar](100) NULL,
	[incentive_amt] [varchar](100) NULL,
	[user_ismobiledevice] [bit] NULL,
	[foreign_language] [varchar](100) NULL,
	[DWCHECKSUM] [bigint] NULL,
 CONSTRAINT [PK_HouseholdDim2015] PRIMARY KEY CLUSTERED 
(
	[HouseholdDimID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


