DROP TABLE [stg].[log]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[log](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[log_level] [int] NULL,
	[log_levelname] [char](32) NULL,
	[log] [char](2048) NOT NULL,
	[lineno] int NULL,
	[module] varchar(255) NULL,
	[pathname] nvarchar(max) NULL,
	[created_at] [datetime2](7) NOT NULL,
	[created_by] [char](32) NOT NULL
) ON [PRIMARY]
GO