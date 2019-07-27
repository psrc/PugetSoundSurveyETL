SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[Mapping_File](
	[Master_Names] [nvarchar](50) NOT NULL,
	[DataType] [nvarchar](50) NULL,
	[file_2015] [nvarchar](50) NULL,
	[file_2017] [nvarchar](50) NULL,
	[file_2019] [nvarchar](50) NULL
) ON [PRIMARY]
GO