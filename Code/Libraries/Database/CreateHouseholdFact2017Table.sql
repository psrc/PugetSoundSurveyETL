USE [Elmer]
GO

/****** Object:  Table [HHSurvey].[HouseholdFact2017]    Script Date: 9/16/2019 12:58:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
drop table if exists HHSurvey.HouseholdFact2017

CREATE TABLE [HHSurvey].[HouseholdFact2017](
	[HouseholdFactID] [bigint] NOT NULL,
	[HouseholdDimID] [bigint] NOT NULL,
	[hh_wt_revised] [float] NULL,
	[hh_day_wt_revised] [float] NULL,
	[DWCHECKSUM] [bigint] NULL,
 CONSTRAINT [PK_HouseholdFact2017] PRIMARY KEY CLUSTERED 
(
	[HouseholdFactID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


