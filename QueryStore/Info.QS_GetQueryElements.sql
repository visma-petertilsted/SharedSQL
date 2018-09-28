-- ================================================================================================
-- QS_GetQueryElements – Returnerer de query_id(s) benyttet af specificeret objekt
-- ================================================================================================
USE DinDatabaseMedQueryStore
GO


CREATE OR ALTER PROCEDURE [Info].[QS_GetQueryElements] 
(
   @help			    TINYINT			= 0,
   @startdate	 	    DATETIME		= NULL,
   @enddate			    DATETIME		= NULL,
   @StoredProcName	    NVARCHAR(100)	= NULL,		
   @QueryID			    INT				= NULL,
   @PlanID			    INT				= NULL
)

AS

BEGIN

    DECLARE @use_startdate DATETIMEOFFSET(7),
		  @use_enddate DATETIMEOFFSET(7),
		  @sProcedureName NVARCHAR(100),
		  @nQueryID int,
		  @nPlanID int,
		  @sqlText NVARCHAR(MAX),
		  @sWhereClause NVARCHAR(200)

	SELECT @sqlText = '',
			  @sProcedureName = @StoredProcName,
			  @nQueryID = @QueryID,
			  @nPlanID =  @PlanID,
			  @sWhereClause = '';
		  
   IF @Help = 1 
    PRINT '
-------------------------------------------------------------------------
-- QS_GetQueryElements
-------------------------------------------------------------------------
Returns the objectname, query_id, plan_id combination used by databaseobjects in the given timeperiod
Mostly used to find queryid used by databaseobject, but any of the 3 can be used as parameter

Resultset also includes various information about the plan that could prove usefull

Default with no parameters added, is from the latest hour

Possible parameters are:

@help		TINYINT				Default 0
	If 1 Show this documentation 

@startdate	DATETIME			Default GETDATE() minus 1 hour
	Enter the startdate of the datacollection
	Enter the date as a datetime string or any substring hereoff
	''2017-12-06 15:47:43.440'' or ''2017-12-06'' etc.

@enddate DATETIME				Default GETDATE()
	Enter the enddate of the datacollection
	Enter the date as a datetime string or any substring hereoff
	''2017-12-06 15:47:43.440'' or ''2017-12-06'' etc.

Only one of the following should be used to filter at the time

@StoredProcName NVARCHAR(100)	Default NULL 	
	If entered, only results that matches the value is returned

@QueryID		INT				Default NULL 	
	If entered, only results that matches the value is returned

@PlanID			INT				Default NULL 	
	If entered, only results that matches the value is returned
-------------------------------------------------------------------------
    ';

	--First some parameter validation
    IF (@startdate is NULL  AND @enddate is NULL )
	   BEGIN
		  SELECT @use_startdate	 = TODATETIMEOFFSET (DATEADD(hh,-2,GETDATE()), '+00:00')
		  SELECT @use_enddate	 = TODATETIMEOFFSET (DATEADD(hh,-1,GETDATE()), '+00:00')
	   END
    ELSE IF (@startdate is not NULL  AND @enddate  is not NULL )
	   BEGIN
		  SELECT @use_startdate	 = TODATETIMEOFFSET (@startdate, '+00:00')
		  SELECT @use_enddate	 = TODATETIMEOFFSET (@enddate, '+00:00')
	   END
    ELSE
	   BEGIN
		  PRINT 'Alle datoer eller ingen datoer skal være udfyldt';
		  RETURN;
	   END

	--start building the sql to run
    IF @sProcedureName is not NULL 
	   SELECT @sWhereClause = 'and object_name(qsq.object_id) = '''+ @sProcedureName+''''

    IF @nQueryID is not NULL 
	   SELECT @sWhereClause = 'and qsq.query_id= '+ CAST(@nQueryID AS NVARCHAR(10))+''

    IF @nPlanID is not NULL 
	   SELECT @sWhereClause = 'and qsp.plan_id = '+ CAST(@nPlanID AS NVARCHAR(10))+''

	SELECT @sqlText =
	'
			SELECT  distinct object_name(qsq.object_id), qsp.query_id, qsp.plan_id
			, qsp.is_trivial_plan
			, qsp.is_parallel_plan
			, qsp.is_forced_plan	
			, qsp.force_failure_count
			, qsp.last_force_failure_reason
			, qsp.is_natively_compiled	
			, qsp.compatibility_level 
			FROM    sys.query_store_query AS qsq
			JOIN    sys.query_store_plan AS qsp
					ON qsp.query_id = qsq.query_id
			JOIN    sys.query_store_runtime_stats AS qsrs
					ON qsrs.plan_id = qsp.plan_id
			JOIN    sys.query_store_runtime_stats_interval AS rsi
					ON rsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
			WHERE 1=1
			and rsi.start_time > @startdate
			and rsi.end_time < @enddate
			and object_name(qsq.object_id) is not null
			 '+@sWhereClause+' 
			ORDER BY object_name(qsq.object_id), qsp.query_id
	'

	--print @sqltext

	exec sp_executesql @sqlText,
    N'@startdate datetime,@enddate datetime',
    @startdate=@use_startdate,
    @enddate=@use_enddate


END
GO
