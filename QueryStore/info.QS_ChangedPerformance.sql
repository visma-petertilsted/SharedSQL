
-- ================================================================================================
-- [info].[QS_ChangedPerformance] -Returnerer dataset svarende til rapporten 'Regressed queries'
-- ================================================================================================

USE DinDatabaseMedQuerystore
GO

CREATE OR ALTER PROCEDURE [info].[QS_ChangedPerformance] 
(
   @help			    TINYINT		= 0,
   @resultrows		    INT			= 25,	
   @recent_start	    DATETIME		= NULL,
   @recent_end		    DATETIME		= NULL,
   @history_start	    DATETIME		= NULL,
   @history_end	    DATETIME		= NULL,
   @Measure		    NVARCHAR(30)	= N'duration',
   @MeasureType	    NVARCHAR(5)	= N'total',
   @MeasureDirection    NVARCHAR(10)	= N'negativ',
   @ProcedureName	    NVARCHAR(100)	= NULL,
   @queryID		    INT			= NULL		
)

AS

BEGIN

    DECLARE @nResultrows int,
		  @use_recent_start_time datetimeoffset(7),
		  @use_recent_end_time datetimeoffset(7),
		  @use_history_start_time datetimeoffset(7),
		  @use_history_end_time datetimeoffset(7),
		  @min_exec_count bigint,
		  @sqlText NVARCHAR(MAX),
		  @sMeasure NVARCHAR(30),
		  @sMeasureType NVARCHAR(5),
		  @sMeasureDirection NVARCHAR(10),
		  @sMeasureFormula NVARCHAR(50),
		  @sQueryIDFilter NVARCHAR(50),
		  @sWhereClause NVARCHAR(200),
		  @sResultColumn1 NVARCHAR(200), 
		  @sProcedureName NVARCHAR(100),
		  @ProcQueryID int,
		  @nQueryID int,
		  @sResultColumn2 NVARCHAR(300)

    SELECT
	 @nResultrows = @resultrows,
	 @min_exec_count = 1,
	 @sqlText = '',
	 @sMeasure = @Measure,
	 @sMeasureType = @MeasureType,
	 @sMeasureDirection = @MeasureDirection,
	 @sMeasureFormula = '',
	 @sResultColumn1 = '',
	 @sResultColumn2 = '',
	 @sQueryIDFilter = '',
	 @sProcedureName = @ProcedureName,
	 @nQueryID= @queryID,
	 @Help = 1,
	 @sWhereClause = '';

    IF @Help = 1 
    PRINT '
    -------------------------------------------------------------------------
    -- ChangedPerformance Queries 
    -- Returnerer det angivne antal rækker med procedurer med størst ændring i performance i det angivne tidsrum
    -- Ændringen kan være både positiv og negativ, hvilken vej der ænskes angives med parametre
    -- 
    -- Reverse engineered query fra query stores ''regressed queries'' graf.
    -- Dog kun for Total og Average
    --
    -- Med denne kan man så istedet få udtrukket til en tabel, og for en given periode
    -- Default er seneste time mod seneste uge
    --
    -- !! Bemærk at hvis procedurenavn eller query_id angives, kan slutresultatet blive tomt, 
    --    hvis det angivne ikke havde en forbedring eller forværring
    --
    --    Datoer angives således: ''2017-12-06 15:47:43.440''
    --
    -- Der kan angives følgende parametre:
    --	   @help TINYINT		
    --		  hvis 1 angives, vises dokumentation	 	 Default 0
    --	   @resultrows INT		
    --		  Hvor mange rækker skal returneres		 Default 25
    --	   @recent_start DATETIME	
    --		  Startdato for seneste periode			 Default getdate()-1 time
    --      @recent_end DATETIME		
    --		  Slutdato for seneste periode			 Default getdate()
    --      @history_start DATETIME	
    --		  Startdato for historisk periode			 Default getdate()-1 uge
    --      @history_end DATETIME
    --		  Slutdato for historisk periode			 Default getdate()
    --	   @Measure  NVARCHAR(30)		
    --		  Hvilken måling skal foretages	   		 Default ''duration''  
    --		  mulige valg: ''duration'',''cpu_time'',''physical_io_reads'',''logical_io_reads'',''logical_io_writes'',''query_max_used_memory''
    --	   @MeasureType  NVARCHAR(5)		
    --		  Skal beregnes total eller gennemsnit		 Default ''total''  
    --		  mulige valg: ''total'',''avg''
    --	   @MeasureDirection  NVARCHAR(10)		
    --		  skal der ses de bedste fremgange
    --		  eller de største forværringer	   		 Default ''negativ''  
    --		  mulige valg: ''positiv'',''negativ''
    --	   @ProcedureName NVARCHAR(100)	
    --		  Navn på Stored procedure der ønskes status for
    --		  Hvis angivet returneres kun for denne	  
    --		  Uanset hvor stor forbedring/forværring er	 Default NULL 
    --	   @queryID INT		
    --		  kendes specifik qery_id kan den angives	 Default NULL
    --
    --
    -------------------------------------------------------------------------
    ';

    IF (@recent_start is NULL  AND @recent_end is NULL AND @history_start is NULL AND  @history_end is NULL)
	   BEGIN
		  SELECT @use_recent_start_time	 = TODATETIMEOFFSET (DATEADD(hh,-1,GETDATE()), '+01:00')
		  SELECT @use_recent_end_time		 = TODATETIMEOFFSET (GETDATE(), '+01:00')
		  SELECT @use_history_start_time	 = TODATETIMEOFFSET (DATEADD(dd,-7,GETDATE()), '+01:00')
		  SELECT @use_history_end_time	 = TODATETIMEOFFSET (GETDATE(), '+01:00')
	   END
    ELSE IF (@recent_start is not NULL  AND @recent_end  is not NULL AND   @history_start is not NULL AND  @history_end is not NULL)
	   BEGIN
		  -- her burde være et check af de indtastede datoer
		  SELECT @use_recent_start_time	 = TODATETIMEOFFSET (DATEADD(hh,-1,@recent_start), '+01:00')
		  SELECT @use_recent_end_time	 = TODATETIMEOFFSET (@recent_end, '+01:00')
		  SELECT @use_history_start_time	 = TODATETIMEOFFSET (DATEADD(dd,-7,@history_start), '+01:00')
		  SELECT @use_history_end_time	 = TODATETIMEOFFSET (@history_end, '+01:00')
	   END
    ELSE
	   BEGIN
		  PRINT 'Alle datoer eller ingen datoer skal være udfyldt';
		  RETURN;
	   END


    IF @sMeasure not in ('duration','cpu_time','physical_io_reads','logical_io_reads','logical_io_writes','query_max_used_memory')
	   BEGIN
    		  PRINT 'Den valgte måling '+@sMeasure+' findes ikke.
		  Mulige værdier: duration, cpu_time, physical_io_reads, logical_io_reads, logical_io_writes, query_max_used_memory';
		  RETURN;
	   END

    IF @sMeasureType not in ('total','avg')
	   BEGIN
    		  PRINT 'Den valgte målingstype '+@sMeasureType+' findes ikke.
		  Mulige værdier: total, avg';
		  RETURN;
	   END

    IF @sMeasureDirection not in ('positiv','negativ')
	   BEGIN
    		  PRINT 'Den valgte ændringstype '+@sMeasureType+' findes ikke.
		  Mulige værdier: positiv,negativ';
		  RETURN;
	   END

    IF @sProcedureName is not NULL 
	   BEGIN
		  --find en query i QueryStore
		  SELECT  top 1 @ProcQueryID = qsp.query_id -- vi skal bare vide om den fandtes i perioden
		  FROM    sys.query_store_query AS qsq
		  JOIN    sys.query_store_plan AS qsp
				ON qsp.query_id = qsq.query_id
		  JOIN    sys.query_store_runtime_stats AS qsrs
				ON qsrs.plan_id = qsp.plan_id
		  JOIN    sys.query_store_runtime_stats_interval AS rsi
				ON rsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
		  WHERE 1=1
		  and rsi.start_time > @use_recent_start_time
		  and rsi.end_time < @use_recent_end_time
		  and object_name(qsq.object_id) = @sProcedureName

		  IF @ProcQueryID is NULL  
			 BEGIN
				PRINT 'Procedure '+@sProcedureName+' er ikke eksekveret i seneste angivne periode';
				RETURN;
			 END
	   END

    IF @nQueryID is not NULL 
	   BEGIN
		  --find en query i QueryStore
		  SELECT  top 1 @ProcQueryID = qsp.query_id -- vi skal bare vide om den fandtes i perioden
		  FROM    sys.query_store_query AS qsq
		  JOIN    sys.query_store_plan AS qsp
				ON qsp.query_id = qsq.query_id
		  JOIN    sys.query_store_runtime_stats AS qsrs
				ON qsrs.plan_id = qsp.plan_id
		  JOIN    sys.query_store_runtime_stats_interval AS rsi
				ON rsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
		  WHERE 1=1
		  and rsi.start_time > @use_recent_start_time
		  and rsi.end_time < @use_recent_end_time
		  and qsp.query_id = @nQueryID

		  IF @ProcQueryID is NULL  
			 BEGIN
				PRINT 'Query_id '+CAST(@nQueryID AS NVARCHAR(20))+' er ikke eksekveret i seneste angivne periode';
				RETURN;
			 END
		  ELSE
			 BEGIN
				SELECT @sQueryIDFilter = ' and p.query_id = '+CAST(@nQueryID AS NVARCHAR(20))
			 END
	   END

    -- total		  resultcolumn1 'results.additional_cpu_time_workload additional_cpu_time_workload,'
    -- total positiv resultcolumn1 'results.reduced_cpu_time_workload reduced_cpu_time_workload,'
    -- avg		  resultcolumn1 'results.cpu_time_regr_perc_recent cpu_time_regr_perc_recent,'
    -- avg   positiv resultcolumn1 'results.cpu_time_impro_perc_recent cpu_time_impro_perc_recent,'

    -- total		  resultcolumn2 'ROUND(CONVERT(float, recent.total_cpu_time / recent.count_executions - hist.total_cpu_time / hist.count_executions) * (recent.count_executions), 2) additional_cpu_time_workload,'
    -- total positiv resultcolumn2 'ROUND(CONVERT(float, hist.total_cpu_time / hist.count_executions - recent.total_cpu_time / recent.count_executions ) * (recent.count_executions), 2) reduced_cpu_time_workload,'
    -- avg		  resultcolumn2 'ROUND(CONVERT(float, recent.avg_cpu_time - hist.avg_cpu_time) / NULLIF(hist.avg_cpu_time, 0) * 100.0, 2) cpu_time_regr_perc_recent,'
    -- avg positiv   resultcolumn2 'ROUND(CONVERT(float, hist.avg_cpu_time - recent.avg_cpu_time ) / NULLIF(recent.avg_cpu_time, 0) * 100.0, 2) cpu_time_impro_perc_recent,'

	 IF @sMeasureType = 'total'
	   BEGIN
		  IF @sMeasure in ('duration','cpu_time')
			 BEGIN 
				SELECT @sMeasureFormula = '  * 0.001, 2)'
			 END
		  ELSE
			 BEGIN
				SELECT @sMeasureFormula = '   * 8, 2)'
			 END
		  IF @sProcedureName is not NULL
			 BEGIN
			   SELECT @sWhereClause = 'WHERE additional_'+@sMeasure+'_workload > 0 AND ISNULL(OBJECT_NAME(results.object_id), '''') = '''+@sProcedureName+''' ORDER BY additional_'+@sMeasure+'_workload DESC'
			 END
		  ELSE
			 BEGIN
			   SELECT @sWhereClause = 'WHERE additional_'+@sMeasure+'_workload > 0 ORDER BY additional_'+@sMeasure+'_workload DESC'
			 END

		  IF @sMeasureDirection = 'positiv'
			 BEGIN
				SELECT @sWhereClause = REPLACE(@sWhereClause,'additional','reduced')
			 END

		  IF @sMeasureDirection = 'negativ'
			 BEGIN
			   SELECT @sResultColumn1 =  'results.additional_'+@sMeasure+'_workload additional_'+@sMeasure+'_workload,'
			   SELECT @sResultColumn2 =  'ROUND(CONVERT(float, recent.total_'+@sMeasure+' / recent.count_executions - hist.total_'+@sMeasure+' / hist.count_executions) * (recent.count_executions), 2) additional_'+@sMeasure+'_workload,'
			 END
		  ELSE IF @sMeasureDirection = 'positiv'
			 BEGIN
			   SELECT @sResultColumn1 =  'results.reduced_'+@sMeasure+'_workload reduced_'+@sMeasure+'_workload,'
			   SELECT @sResultColumn2 =  'ROUND(CONVERT(float, hist.total_'+@sMeasure+' / hist.count_executions - recent.total_'+@sMeasure+' / recent.count_executions) * (recent.count_executions), 2) reduced_'+@sMeasure+'_workload,'
			 END
	   END
   
	 IF @sMeasureType = 'avg'
	   BEGIN
		  IF @sMeasure in ('duration','cpu_time')
			 BEGIN 
				SELECT @sMeasureFormula = '  / NULLIF(SUM(rs.count_executions), 0) * 0.001, 2)'
			 END
		  ELSE
			 BEGIN
				SELECT @sMeasureFormula = '  / NULLIF(SUM(rs.count_executions), 0) * 8, 2)'
			 END
		  IF @sProcedureName is not NULL
			 BEGIN
			   SELECT @sWhereClause = 'WHERE '+@sMeasure+'_regr_perc_recent > 0 AND ISNULL(OBJECT_NAME(results.object_id), '''') = '''+@sProcedureName+''' ORDER BY '+@sMeasure+'_regr_perc_recent DESC'
			 END
		  ELSE
			 BEGIN
			   SELECT @sWhereClause = 'WHERE '+@sMeasure+'_regr_perc_recent > 0 ORDER BY '+@sMeasure+'_regr_perc_recent DESC'
			 END

		  IF @sMeasureDirection = 'positiv'
			 BEGIN
				SELECT @sWhereClause = REPLACE(@sWhereClause,'regr_','impro_')
			 END

		  IF @sMeasureDirection = 'negativ'
			 BEGIN
			   SELECT @sResultColumn1 =  'results.'+@sMeasure+'_regr_perc_recent '+@sMeasure+'_regr_perc_recent,'
			   SELECT @sResultColumn2 =  'ROUND(CONVERT(float, recent.avg_'+@sMeasure+' - hist.avg_'+@sMeasure+') / NULLIF(hist.avg_'+@sMeasure+', 0) * 100.0, 2) '+@sMeasure+'_regr_perc_recent,'
			 END
		  ELSE IF @sMeasureDirection = 'positiv'
			 BEGIN
			   SELECT @sResultColumn1 =  'results.'+@sMeasure+'_impro_perc_recent '+@sMeasure+'_impro_perc_recent,'
			   SELECT @sResultColumn2 =  'ROUND(CONVERT(float, hist.avg_'+@sMeasure+' - recent.avg_'+@sMeasure+' ) / NULLIF(recent.avg_'+@sMeasure+', 0) * 100.0, 2) '+@sMeasure+'_impro_perc_recent,'
			 END
	   END
   
    SELECT @sqlText ='

    WITH hist
    AS (SELECT
	 p.query_id query_id,
	 ROUND(CONVERT(float, SUM(rs.avg_'+@sMeasure+' * rs.count_executions))'+@sMeasureFormula+' '+@sMeasureType+'_'+@sMeasure+',
	 SUM(rs.count_executions) count_executions,
	 COUNT(DISTINCT p.plan_id) num_plans
    FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p
	 ON p.plan_id = rs.plan_id
    WHERE NOT (rs.first_execution_time > @history_end_time
    OR rs.last_execution_time < @history_start_time) '+@sQueryIDFilter+'
    GROUP BY p.query_id),
    recent
    AS (SELECT
	 p.query_id query_id,
	 ROUND(CONVERT(float, SUM(rs.avg_'+@sMeasure+' * rs.count_executions))'+@sMeasureFormula+' '+@sMeasureType+'_'+@sMeasure+',
	 SUM(rs.count_executions) count_executions,
	 COUNT(DISTINCT p.plan_id) num_plans
    FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p
	 ON p.plan_id = rs.plan_id
    WHERE NOT (rs.first_execution_time > @recent_end_time
    OR rs.last_execution_time < @recent_start_time) '+@sQueryIDFilter+'
    GROUP BY p.query_id)

    SELECT TOP (@results_row_count)
	 results.query_id query_id,
	 results.object_id object_id,
	 ISNULL(OBJECT_NAME(results.object_id), '''') object_name,
	 results.query_sql_text query_sql_text,
	 '+@sResultColumn1+'
	 results.'+@sMeasureType+'_'+@sMeasure+'_recent '+@sMeasureType+'_'+@sMeasure+'_recent,
	 results.'+@sMeasureType+'_'+@sMeasure+'_hist '+@sMeasureType+'_'+@sMeasure+'_hist,
	 ISNULL(results.count_executions_recent, 0) count_executions_recent,
	 ISNULL(results.count_executions_hist, 0) count_executions_hist,
	 queries.num_plans num_plans
    FROM (SELECT
	 hist.query_id query_id,
	 q.object_id object_id,
	 qt.query_sql_text query_sql_text,
	 '+@sResultColumn2+'
	 ROUND(recent.'+@sMeasureType+'_'+@sMeasure+', 2) '+@sMeasureType+'_'+@sMeasure+'_recent,
	 ROUND(hist.'+@sMeasureType+'_'+@sMeasure+', 2) '+@sMeasureType+'_'+@sMeasure+'_hist,
	 recent.count_executions count_executions_recent,
	 hist.count_executions count_executions_hist
    FROM hist
    JOIN recent
	 ON hist.query_id = recent.query_id
    JOIN sys.query_store_query q
	 ON q.query_id = hist.query_id
    JOIN sys.query_store_query_text qt
	 ON q.query_text_id = qt.query_text_id
    WHERE recent.count_executions >= @min_exec_count) AS results
    JOIN (SELECT
	 p.query_id query_id,
	 COUNT(DISTINCT p.plan_id) num_plans
    FROM sys.query_store_plan p
    GROUP BY p.query_id
    HAVING COUNT(DISTINCT p.plan_id) >= 1) AS queries
	 ON queries.query_id = results.query_id
    '+@sWhereClause+'-- OPTION (MERGE JOIN);'

--  print @sqlText

    exec sp_executesql @sqlText,
    N'@results_row_count int,@recent_start_time datetimeoffset(7),@recent_end_time datetimeoffset(7),@history_start_time datetimeoffset(7),@history_end_time datetimeoffset(7),@min_exec_count bigint',
    @results_row_count=@nResultrows,
    @recent_start_time=@use_recent_start_time,
    @recent_end_time=@use_recent_end_time,
    @history_start_time=@use_history_start_time,
    @history_end_time=@use_history_end_time,
    @min_exec_count=1

END
GO

PRINT 'PROCEDURE [info].[QS_ChangedPerformance] ALTERED'
GO

GRANT EXECUTE on [info].[QS_ChangedPerformance] to PUBLIC