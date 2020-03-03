/****** Object:  StoredProcedure [dbo].[mergeHouseHoldDim2015]    Script Date: 8/12/2019 9:03:52 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROC [dbo].[mergeHouseHoldDim2015] 
AS
BEGIN


-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
--	NAME        : [stg].[mergeHouseHoldDim2015] 
--	CREATED BY  : William Andrus
--	DATE        : 2019-07-29
--	DESCRIPTION : Merge Household Dim Table for 2015 
--	NOTES       : none
--	USAGE		: 
-----------------------------------------------------------------------------------------------------------------------
--	VER:    DATE:       NAME:           DESCRIPTION OF CHANGE:
--	------- ----------- --------------- -------------------------------------------------------------------------------
--	1.0     2019.07.29	William Andrus	Initial Creation
-----------------------------------------------------------------------------------------------------------------------

;with cte as 
(
	SELECT 
	cast([hhid] as int) as HouseholdDimID
	,[hhid] as HouseholdID
	,max(cast([pernum] as tinyint))  as HouseholdSize
	,BINARY_CHECKSUM(hhid, max(pernum)) as DWCHECKSUM
	FROM [stg].[ResponseAndCode_2015] person
	group by [hhid])
MERGE dbo.HouseholdDim AS target  
USING cte AS source
ON 
(
	target.HouseholdDimID = source.HouseholdDimID
)  
WHEN MATCHED AND target.DWCHECKSUM <> source.DWCHECKSUM THEN
	UPDATE SET
      target.HouseholdSize = source.HouseholdSize
      ,target.[DWCHECKSUM] = source.DWCHECKSUM
WHEN NOT MATCHED THEN 
	INSERT 
    (
		HouseholdDimID
		,HouseholdID
        ,HouseholdSize
		,[DWCHECKSUM]
	)
     VALUES
     (
	  	source.HouseholdDimID
		,source.HouseholdID
        ,source.HouseholdSize
		,source.[DWCHECKSUM]
	  );


END


GO

