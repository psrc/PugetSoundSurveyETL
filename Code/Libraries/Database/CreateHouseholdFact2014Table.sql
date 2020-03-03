USE [Elmer]
GO

/****** Object:  Table [HHSurvey].[HouseholdDim2014]    Script Date: 9/16/2019 12:13:48 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

drop table if exists HHSurvey.HouseholdFact2014

CREATE TABLE [HHSurvey].[HouseholdFact2014](
	[HouseholdDimID] [bigint] NOT NULL,
	[expwt_2] [float] NULL,
	[DWCHECKSUM] [bigint] NULL,
 CONSTRAINT [PK_HouseholdFact2014] PRIMARY KEY CLUSTERED 
(
	[HouseholdDimID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


