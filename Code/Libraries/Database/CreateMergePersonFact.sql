/****** Object:  StoredProcedure [dbo].[mergePersonFact2015]    Script Date: 8/12/2019 10:00:24 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROC [dbo].[mergePersonFact2015] 
AS
BEGIN


-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
--	NAME        : [dbo].[mergePersonFact2015] 
--	CREATED BY  : William Andrus
--	DATE        : 2019-07-29
--	DESCRIPTION : Merge Person Fact Table for 2015 
--	NOTES       : none
--	USAGE		: 
-----------------------------------------------------------------------------------------------------------------------
--	VER:    DATE:       NAME:           DESCRIPTION OF CHANGE:
--	------- ----------- --------------- -------------------------------------------------------------------------------
--	1.0     2019.07.29	William Andrus	Initial Creation
-----------------------------------------------------------------------------------------------------------------------

;with cte as 
(
	SELECT DISTINCT
	cast([personid] as int) as PersonDimID
	,cast([hhid] as int) as HouseholdDimID
	,null as TravelDateDimID
	,cast(cast(numtrips as decimal(4,2)) as int) as NumTrips
	,cast(cast(REPLACE(diary_duration_minutes,'nan','-1') as decimal(10,2)) as int) as DiaryDurationMinutes
	,BINARY_CHECKSUM([personid],hhid,NumTrips,diary_duration_minutes) AS DWCHECKSUM
	FROM [stg].[ResponseAndCode_2015] 
)
MERGE dbo.PersonFact AS target  
USING cte AS source
ON 
(
	target.PersonDimID = source.PersonDimID
	AND target.HouseholdDimID = source.HouseholdDimID
	AND coalesce(target.TravelDateDimID,-1) = coalesce(source.TravelDateDimID,-1)
)  
WHEN MATCHED AND target.DWCHECKSUM <> source.DWCHECKSUM THEN
	UPDATE SET
      target.NumTrips = source.NumTrips 
      ,target.DiaryDurationMinutes = source.DiaryDurationMinutes 
      ,target.[DWCHECKSUM] = source.DWCHECKSUM
WHEN NOT MATCHED THEN 
	INSERT 
    (
		PersonDimID
		,HouseholdDimID
        ,TravelDateDimID
		,NumTrips
		,DiaryDurationMinutes
        ,[DWCHECKSUM]
	)
     VALUES
     (
	   source.PersonDimID
       ,source.HouseholdDimID 
       ,source.TravelDateDimID 
       ,source.NumTrips
	   ,source.DiaryDurationMinutes
       ,source.DWCHECKSUM
	  );


END


GO

