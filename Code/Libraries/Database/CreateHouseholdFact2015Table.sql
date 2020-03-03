USE [Elmer]
GO

/****** Object:  Table [HHSurvey].[HouseholdFact2015]    Script Date: 9/16/2019 1:10:05 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

drop table if exists HHSurvey.HouseholdFact2015

CREATE TABLE [HHSurvey].[HouseholdFact2015](
	[HouseholdFactID] [bigint] NOT NULL,
	[HouseholdDimID] [bigint] NOT NULL,
	[expwt_h1415] [float] NULL,
	[DWCHECKSUM] [bigint] NULL,
 CONSTRAINT [PK_HouseholdFact2015] PRIMARY KEY CLUSTERED 
(
	[HouseholdFactID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


