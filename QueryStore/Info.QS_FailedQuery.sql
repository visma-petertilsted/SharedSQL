-- ================================================================================================
-- QS_GetQueryElements – Returnerer fejlede queries
-- ================================================================================================
USE DinDatabaseMedQueryStore
GO

CREATE OR ALTER PROCEDURE [Info].[QS_FailedQuery] 
(
   @help			    TINYINT		= 0,
   @startdate	 	    DATETIME		= NULL,
   @enddate				DATETIME		= NULL,
   @datoFormat		    VARCHAR(20)	= 'yyyy-MM-dd:HH',
   @StoredProcName	    NVARCHAR(100)	= NULL		
)

AS

BEGIN

    DECLARE @use_startdate DATETIMEOFFSET(7),
		  @use_enddate DATETIMEOFFSET(7),
		  @sProcedureName NVARCHAR(100),
		  @ProcQueryID int,
		  @sqlText NVARCHAR(MAX),
		  @sWhereClause NVARCHAR(200)

	SELECT
		 @sqlText = '',
		 @sProcedureName = @StoredProcName,
		 @sWhereClause = '';
		  
   IF @Help = 1 
    PRINT '
-------------------------------------------------------------------------
-- QS_FailedQuery
-------------------------------------------------------------------------
Returns databaseobjects that did not complete with succes
Aborted - Query failed
Exception - Query stopped for other reasons, have only found the reason to be timeout

Default with no parameters added, is from the latest hour

Possible parameters are:

@help		TINYINT				Default 0
	If 1 Show this documentation 

@startdate	DATETIME			Default GETDATE() minus 1 hour
	Enter the startdate of the datacollection
	Enter the date as a datetime string or any sustring hereoff
	''2017-12-06 15:47:43.440'' or ''2017-12-06'' etc.

@enddate DATETIME				Default GETDATE()
	Enter the enddate of the datacollection
	Enter the date as a datetime string or any sustring hereoff
	''2017-12-06 15:47:43.440'' or ''2017-12-06'' etc.

@datoFormat		VARCHAR(20)		Default ''yyyy-MM-dd:HH''
	Enter the string used to group the extracted data by
	any string that is allowed for formatting a date using FORMAT() is allowed
	''yyyy-MM-dd:HH'' or ''yyyy-MM-dd'' or ''yyyy'' etc

@StoredProcName NVARCHAR(100)	Default NULL 	
	If entered, only results that matches the value is returned
-------------------------------------------------------------------------
    ';

	--First som parameter checking
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

	SELECT @sqlText ='
			SELECT  FORMAT(rsi.start_time, '''+@datoFormat+''') AS tid ,
					object_name(qsq.object_id) AS ObjektNavn,
					qsq.query_id,
					qsp.plan_id,
					qsrs.execution_type_desc,
					sum(qsrs.count_executions)
			FROM    sys.query_store_query AS qsq
			JOIN    sys.query_store_plan AS qsp
						ON qsp.query_id = qsq.query_id
			JOIN    sys.query_store_runtime_stats AS qsrs
						ON qsrs.plan_id = qsp.plan_id
			JOIN    sys.query_store_runtime_stats_interval AS rsi
						ON rsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
			WHERE 1=1
			and object_name(qsq.object_id) is not null
			and qsrs.execution_type_desc <> ''Regular''
			and rsi.start_time > @startdate
			and rsi.end_time < @enddate
			 '+@sWhereClause+' 
			group by   FORMAT(rsi.start_time, '''+@datoFormat+'''),
						qsq.query_id,
						qsp.plan_id,
					object_name(qsq.object_id),
					qsrs.execution_type_desc
			order by tid, ObjektNavn
	'
--	print @sqltext

	exec sp_executesql @sqlText,
    N'@startdate datetime,@enddate datetime',
    @startdate=@use_startdate,
    @enddate=@use_enddate
    
END

GO
