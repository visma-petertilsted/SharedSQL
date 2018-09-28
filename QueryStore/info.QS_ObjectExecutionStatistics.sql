-- ================================================================================================
-- QS_ObjectExecutionStatistics - Returnerer dataset svarende til rapporten 'Object Execution Statistics' 
-- som kan køres I SSMS ved at bruge querystore
-- ================================================================================================
USE DinDatabaseMedQueryStore
GO

-------------------------------------------------------------------------
-- Grov førsteversion af stored procedure der finder statistik svarende 
-- til rapporten 'Object Execution Statistics', blot fra Query Store
--
-- stadig debug linier med mere til videreudvikling der kan fjernes når den er færdig
--
-- skal kunne tage dato og mere som parametre, men i første omgang er der to parametre
-- @RunMode nvarchar(5)
--
-- Den kan være 'Day' eller 'Week', Default 'Day', ingen kontrol af indtastet så hvis fejl, bliver det day
-- Day giver statistik for de seneste 24 timer, fordelt på timer
-- Week giver statistik for den seneste uge fordelt på dage
--
-- @DetailMode INT, default 0
-- 0= ingen detaljer
-- 1= detaljer for den periode der er valgt med @RunMode
--
-- @StoredProcName NVARCHAR(1000), default NULL
-- Hvis angivet filtreres udtrækket (normal/detail) for den periode der er valgt med @RunMode, på @StoredProcName
--
-- Mangler:
--    Dynamisk SQL der tager hensyn til valgte parametre
--       Det er påbegyndt med @StoredProcName, men skal udbygges, bør kunne fjerne 50% af linierne
--    udtrækstidspunkt i kolonneoverskrift for rapporteringsperiode
--    opbygning af historik
--    måling af baseline
--    gemme detailudskriftet permanent -- nu valgt som en parameter
--    dato som parameter
--    sorteringskolonne som parameter
--    specifik query som parameter (id)
--    specifik plan som parameter (id)
--    konverting af dato til datetimeoffset, ser ud til at det nuværende er en time forskudt
--    objektnavn som parameter
-------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [Info].[QS_ObjectExecutionStatistics] 
(
	@RunMode NVARCHAR(5) = N'Day', 
	@DetailMode int = 0,
	@StoredProcName NVARCHAR(1000) = NULL
)
AS
BEGIN
	--	DECLARE @RunMode nvarchar(5)
	--	set @RunMode = 'Week'

	DECLARE @sql_select NVARCHAR(MAX) = N'', -- bruges til SELECT statements for dynamisk SQL
		   @sql_where NVARCHAR(MAX) = N''   -- bruges til WHERE clause for dynamisk SQL

	SET NOCOUNT ON
	IF @RunMode IS NULL
		SET @RunMode = 'Day'

	IF @DetailMode IS NULL or @DetailMode > 1
		SET @DetailMode = 0  --default er totaludtræk

	DECLARE @Start DATETIME
	DECLARE @Slut DATETIME
	IF UPPER(@RunMode) = 'WEEK'
		BEGIN
			SET @Start = DateAdd(d, - 7, GETDATE())
			SET @Slut = GETDATE()
		END
	ELSE
		BEGIN
			SET @Start = DateAdd(hh, - 24, GETDATE())
			SET @Slut = GETDATE()
		END

	--select @Start, @Slut

	SELECT DB_NAME() AS DBName
		,CASE 
			WHEN s.name IS NULL
				THEN 'Andet' + '_' + rtrim(convert(CHAR(10), p.query_id))
			ELSE QUOTENAME(s.name) + '.' + QUOTENAME(o.name)
			END AS ObjectName
		,ISNULL(o.type_desc, 'ukendt') ObjektType
		,p.query_id
		,p.plan_id
		,CAST(rsi.start_time AS DATETIME) AS interval_start_time
		,rs.runtime_stats_interval_id
		,rs.count_executions
		,rs.avg_cpu_time
		,rs.avg_cpu_time * rs.count_executions cpu_per_interval
		,rs.avg_duration
		,rs.avg_duration * rs.count_executions duration_per_interval
		,rs.avg_query_max_used_memory
		,rs.avg_query_max_used_memory * rs.count_executions mem_per_interval
		,rs.avg_logical_io_reads
		,rs.avg_logical_io_reads * rs.count_executions logical_io_reads_per_interval
		,rs.avg_logical_io_writes
		,rs.avg_logical_io_writes * rs.count_executions logical_io_writes_per_interval
		,rs.avg_physical_io_reads
		,rs.avg_physical_io_reads * rs.count_executions physical_io_reads_per_interval
	--	, rs.*
	INTO #periodexecutions
	FROM sys.query_store_runtime_stats rs
	--(
	--    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan_id, runtime_stats_id ORDER BY runtime_stats_id DESC) AS recent_stats_in_current_priod
	--    FROM sys.query_store_runtime_stats 
	--) AS rs
	INNER JOIN sys.query_store_runtime_stats_interval AS rsi 
	 ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
	INNER JOIN sys.query_store_plan AS p 
	 ON p.plan_id = rs.plan_id
	INNER JOIN sys.query_store_query AS q 
	 ON q.query_id = p.query_id
	INNER JOIN sys.query_store_query_text AS qt 
	 ON qt.query_text_id = q.query_text_id
	LEFT JOIN sys.objects AS o 
	 ON o.object_id = q.object_id
	LEFT JOIN sys.schemas AS s 
	 ON s.schema_id = o.schema_id
	WHERE rsi.start_time > @Start --'2017-12-05 00:00:00.0000000 +00:00'
		AND rsi.end_time < @Slut --'2017-12-06 00:00:00.0000000 +00:00'
		--and s.name is not null

	/*
	SELECT DBName
		,ObjectName
		,query_id
		,plan_id
		,ObjektType
		,YEAR(interval_start_time)*10000 + MONTH(interval_start_time)*100+DAY(interval_start_time) DayPeriod
		,YEAR(interval_start_time)*1000000 + MONTH(interval_start_time)*10000+DAY(interval_start_time)*100+DATEPART(HH,interval_start_time) HourPeriod
		,runtime_stats_interval_id
		,count_executions
		,avg_cpu_time
		,cpu_per_interval
		,avg_duration
		,duration_per_interval
		,avg_query_max_used_memory
		,mem_per_interval
		,avg_logical_io_reads 
		,logical_io_reads_per_interval
		,avg_logical_io_writes
		,logical_io_writes_per_interval
		,avg_physical_io_reads 
		,physical_io_reads_per_interval
	FROM #periodexecutions
	where ObjectName = 'dbo.UF_GetUnemploymentFundMembershipHistory'
	ORDER BY ObjectName,runtime_stats_interval_id,query_id,plan_id
	*/

	SELECT DBName
		,ObjectName
		,MIN(ObjektType) ObjektType
		,MIN(YEAR(interval_start_time) * 10000 + MONTH(interval_start_time) * 100 + DAY(interval_start_time)) DayPeriod
		,MIN(YEAR(interval_start_time) * 1000000 + MONTH(interval_start_time) * 10000 + DAY(interval_start_time) * 100 + DATEPART(HH, interval_start_time)) HourPeriod
		,SUM(count_executions) object_executions_per_interval
		,SUM(cpu_per_interval) object_total_cpu_per_interval  -- microseconds
		,SUM(duration_per_interval) object_total_duration_per_interval -- microseconds
		,SUM(mem_per_interval) object_total_mem_per_interval --number of 8 KB pages
		,SUM(logical_io_reads_per_interval) object_logical_io_reads_per_interval -- number of 8 KB pages
		,SUM(logical_io_writes_per_interval) object_logical_io_writes_per_interval -- number of 8 KB pages
		,SUM(physical_io_reads_per_interval) object_physical_io_reads_per_interval -- number of 8 KB pages
	INTO #periodexecutionsums
	FROM #periodexecutions
	GROUP BY DBName,ObjectName,ObjektType,runtime_stats_interval_id

	--select * from #periodexecutionsums
	/*
	SELECT DBName
		,ObjectName
		,ObjektType
		,DayPeriod
		,HourPeriod
		,object_executions_per_interval
		,object_total_cpu_per_interval
		,object_total_duration_per_interval
		,object_total_mem_per_interval
		,object_logical_io_reads_per_interval
		,object_logical_io_writes_per_interval
		,object_physical_io_reads_per_interval
	FROM #periodexecutionsums
	--GROUP BY DBName, ObjectName, runtime_stats_interval_id
	order by DBName, ObjectName, 3,4
	*/

	-----------------------------
	-- Så har vi samlet data nok til at kunne lave periode rapporten
	-------------------------------
	--Dette burde være lavet med en dynamisk sql, men det må blive senere
	IF UPPER(@RunMode) = 'WEEK'
		BEGIN
			-- først skal vi finde periodens totaler uanset objekt (til at lave 'procent af total' beregning)
			SELECT DBName
				,DayPeriod
				,SUM(object_executions_per_interval) executions_per_reporting_period --den kan vi ikke bruge til noget endnu 
				,SUM(object_total_cpu_per_interval) total_cpu_per_reporting_period
				,SUM(object_total_duration_per_interval) total_duration_per_reporting_period
				,SUM(object_total_mem_per_interval) total_mem_per_reporting_period
				,SUM(object_logical_io_reads_per_interval) total_logical_io_reads_per_reporting_period
				,SUM(object_logical_io_writes_per_interval) total_logical_io_writes_per_reporting_period
				,SUM(object_physical_io_reads_per_interval) total_physical_io_reads_per_reporting_period
			INTO #ReportingPeriodTotalSumsWeek
			FROM #periodexecutionsums
			GROUP BY DBName,DayPeriod

			--select * from #ReportingPeriodTotalSums

			-- og så de enkelte objekters totaler for samme periode
			SELECT DBName
				,ObjectName
				,ObjektType
				,DayPeriod
				,SUM(object_executions_per_interval) object_executions_per_reporting_period
				,SUM(object_total_cpu_per_interval) object_cpu_per_reporting_period
				,SUM(object_total_duration_per_interval) object_duration_per_reporting_period
				,SUM(object_total_mem_per_interval) object_mem_per_reporting_period
				,SUM(object_logical_io_reads_per_interval) object_logical_io_reads_per_reporting_period
				,SUM(object_logical_io_writes_per_interval) object_logical_io_writes_per_reporting_period
				,SUM(object_physical_io_reads_per_interval) object_physical_io_reads_per_reporting_period
			INTO #ReportingPeriodObjectSumsWeek
			FROM #periodexecutionsums
			GROUP BY DBName,ObjectName,ObjektType,DayPeriod

			--select * from #ReportingPeriodObjectSums

			--og så selve beregningerne
			-- første her er for at få en linie med udtrækningstidspunktet
			SELECT NULL Rapporteringsperiode
				,CONVERT(VARCHAR(19), SYSDATETIME(), 120) + ' 7 dage' ObjectName
				,NULL ObjektType
				,NULL object_executions_per_reporting_period
				,NULL avg_cpu_ms
				,NULL Total_cpu_andel
				,NULL Avg_duration_ms
				,NULL Total_duration_andel
				,NULL avg_mem
				,NULL Total_mem_andel
				,NULL avg_logical_reads
				,NULL avg_logical_writes
				,NULL avg_logical_io
				--		,(os.object_logical_io_reads_per_reporting_period + os.object_logical_io_writes_per_reporting_period) total_object_io
				--		,(ts.total_logical_io_reads_per_reporting_period + ts.total_logical_io_writes_per_reporting_period) total_io
				,NULL Total_io_andel
			INTO #ReportingPeriodResultweek
			UNION
			SELECT os.DayPeriod Rapporteringsperiode
				,os.ObjectName
				,os.ObjektType
				,os.object_executions_per_reporting_period
				,CAST((os.object_cpu_per_reporting_period / os.object_executions_per_reporting_period) / 1.0e3 AS DECIMAL(20, 6)) avg_cpu_ms
				,CAST((os.object_cpu_per_reporting_period / ts.total_cpu_per_reporting_period) * 100 AS DECIMAL(20, 6)) Total_cpu_andel
				,CAST((os.object_duration_per_reporting_period / os.object_executions_per_reporting_period) / 1.0e3 AS DECIMAL(20, 6)) Avg_duration_ms
				,CAST((os.object_duration_per_reporting_period / ts.total_duration_per_reporting_period) * 100 AS DECIMAL(20, 6)) Total_duration_andel
				,CAST((os.object_mem_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6))  avg_mem
				,CAST((os.object_mem_per_reporting_period / ts.total_mem_per_reporting_period) * 100 AS DECIMAL(20, 6)) Total_mem_andel
				,CAST((os.object_logical_io_reads_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) avg_logical_reads
				,CAST((os.object_logical_io_writes_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) avg_logical_writes
				,CAST((os.object_logical_io_reads_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) + CAST((os.object_logical_io_writes_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) avg_logical_io
				--		,(os.object_logical_io_reads_per_reporting_period + os.object_logical_io_writes_per_reporting_period) total_object_io
				--		,(ts.total_logical_io_reads_per_reporting_period + ts.total_logical_io_writes_per_reporting_period) total_io
				,CAST(((os.object_logical_io_reads_per_reporting_period + os.object_logical_io_writes_per_reporting_period) / (ts.total_logical_io_reads_per_reporting_period + ts.total_logical_io_writes_per_reporting_period)) * 100 AS DECIMAL(20, 6)) Total_io_andel
			FROM #ReportingPeriodObjectSumsWeek os
			INNER JOIN #ReportingPeriodTotalSumsWeek ts ON os.DayPeriod = ts.DayPeriod

			--Udtræk resultatet
			IF @DetailMode = 0
				BEGIN
				    SET @sql_select = N''
				    SET @sql_where = N''
				    SET @sql_where += N' AND objectname like '''+'%'+@StoredProcName+'%'+''' '

				    SET @sql_select = 
				    N'SELECT Rapporteringsperiode
						,ObjectName
						,ObjektType
						,object_executions_per_reporting_period
						,avg_cpu_ms
						,Total_cpu_andel
						,Avg_duration_ms
						,Total_duration_andel
						,avg_mem
						,Total_mem_andel
						,avg_logical_reads
						,avg_logical_writes
						,avg_logical_io
						,Total_io_andel
					FROM #ReportingPeriodResultWeek
					WHERE 1=1'
					IF @StoredProcName is not null
					   BEGIN
						  SET @sql_select += @sql_where
					   END
					SET @sql_select += N' ORDER BY Rapporteringsperiode,avg_cpu_ms DESC'

--					PRINT @sql_select;

					EXEC sys.sp_executesql  @stmt = @sql_select

					/*
					SELECT Rapporteringsperiode
						,ObjectName
						,ObjektType
						,object_executions_per_reporting_period
						,avg_cpu_ms
						,Total_cpu_andel
						,Avg_duration_ms
						,Total_duration_andel
						,avg_mem
						,Total_mem_andel
						,avg_logical_reads
						,avg_logical_writes
						,avg_logical_io
						,Total_io_andel
					FROM #ReportingPeriodResultWeek
					--where Rapporteringsperiode = 2017120500
					--and ObjectName = 'dbo.UF_GetUnemploymentFundMembershipHistory'
					--skal kunne angives med parameter
					ORDER BY 1,5 DESC
					*/
				END
			ELSE
				BEGIN

				    SET @sql_select = N''
				    SET @sql_where = N''
				    SET @sql_where += N' AND objectname like '''+'%'+@StoredProcName+'%'+''' '

				    SET @sql_select = 
				    N'SELECT DBName
						,ObjectName
						,query_id
						,plan_id
						,ObjektType
						,YEAR(interval_start_time)*10000 + MONTH(interval_start_time)*100+DAY(interval_start_time) DayPeriod
						,YEAR(interval_start_time)*1000000 + MONTH(interval_start_time)*10000+DAY(interval_start_time)*100+DATEPART(HH,interval_start_time) HourPeriod
						,runtime_stats_interval_id
						,count_executions
						,avg_cpu_time
						,cpu_per_interval
						,avg_duration
						,duration_per_interval
						,avg_query_max_used_memory
						,mem_per_interval
						,avg_logical_io_reads 
						,logical_io_reads_per_interval
						,avg_logical_io_writes
						,logical_io_writes_per_interval
						,avg_physical_io_reads 
						,physical_io_reads_per_interval
					FROM #periodexecutions
					WHERE 1=1'
					IF @StoredProcName is not null
					   BEGIN
						  SET @sql_select += @sql_where
					   END

					SET @sql_select += N' ORDER BY ObjectName,runtime_stats_interval_id,query_id,plan_id '

--					PRINT @sql_select;

					EXEC sys.sp_executesql  @stmt = @sql_select


					/*
					SELECT DBName
						,ObjectName
						,query_id
						,plan_id
						,ObjektType
						,YEAR(interval_start_time)*10000 + MONTH(interval_start_time)*100+DAY(interval_start_time) DayPeriod
						,YEAR(interval_start_time)*1000000 + MONTH(interval_start_time)*10000+DAY(interval_start_time)*100+DATEPART(HH,interval_start_time) HourPeriod
						,runtime_stats_interval_id
						,count_executions
						,avg_cpu_time
						,cpu_per_interval
						,avg_duration
						,duration_per_interval
						,avg_query_max_used_memory
						,mem_per_interval
						,avg_logical_io_reads 
						,logical_io_reads_per_interval
						,avg_logical_io_writes
						,logical_io_writes_per_interval
						,avg_physical_io_reads 
						,physical_io_reads_per_interval
					FROM #periodexecutions
					--where ObjectName = 'dbo.UF_GetUnemploymentFundMembershipHistory'
					--skal kunne angives med parameter
					ORDER BY ObjectName,runtime_stats_interval_id,query_id,plan_id
					*/
				END
		END
	ELSE
		BEGIN
			-- først skal vi finde periodens totaler uanset objekt (til at lave 'procent af total' beregning)
			SELECT DBName
				,HourPeriod
				,SUM(object_executions_per_interval) executions_per_reporting_period --den kan vi ikke bruge til noget endnu 
				,SUM(object_total_cpu_per_interval) total_cpu_per_reporting_period
				,SUM(object_total_duration_per_interval) total_duration_per_reporting_period
				,SUM(object_total_mem_per_interval) total_mem_per_reporting_period
				,SUM(object_logical_io_reads_per_interval) total_logical_io_reads_per_reporting_period
				,SUM(object_logical_io_writes_per_interval) total_logical_io_writes_per_reporting_period
				,SUM(object_physical_io_reads_per_interval) total_physical_io_reads_per_reporting_period
			INTO #ReportingPeriodTotalSums
			FROM #periodexecutionsums
			GROUP BY DBName,HourPeriod

			--select * from #ReportingPeriodTotalSums

			-- og så de enkelte objekters totaler for samme periode
			SELECT DBName
				,ObjectName
				,ObjektType
				,HourPeriod
				,SUM(object_executions_per_interval) object_executions_per_reporting_period
				,SUM(object_total_cpu_per_interval) object_cpu_per_reporting_period
				,SUM(object_total_duration_per_interval) object_duration_per_reporting_period
				,SUM(object_total_mem_per_interval) object_mem_per_reporting_period
				,SUM(object_logical_io_reads_per_interval) object_logical_io_reads_per_reporting_period
				,SUM(object_logical_io_writes_per_interval) object_logical_io_writes_per_reporting_period
				,SUM(object_physical_io_reads_per_interval) object_physical_io_reads_per_reporting_period
			INTO #ReportingPeriodObjectSums
			FROM #periodexecutionsums
			GROUP BY DBName,ObjectName,ObjektType,HourPeriod

			--select * from #ReportingPeriodObjectSums

			--og så selve beregningerne
			-- første her er for at få en linie med udtrækningstidspunktet
			SELECT NULL Rapporteringsperiode
				,CONVERT(VARCHAR(19), SYSDATETIME(), 120) + ' 24 timer' ObjectName
				,NULL ObjektType
				,NULL object_executions_per_reporting_period
				,NULL avg_cpu_ms
				,NULL Total_cpu_andel
				,NULL Avg_duration_ms
				,NULL Total_duration_andel
				,NULL avg_mem
				,NULL Total_mem_andel
				,NULL avg_logical_reads
				,NULL avg_logical_writes
				,NULL avg_logical_io
				--		,(os.object_logical_io_reads_per_reporting_period + os.object_logical_io_writes_per_reporting_period) total_object_io
				--		,(ts.total_logical_io_reads_per_reporting_period + ts.total_logical_io_writes_per_reporting_period) total_io
				,NULL Total_io_andel
			INTO #ReportingPeriodResult
			UNION
			SELECT os.HourPeriod Rapporteringsperiode
				,os.ObjectName
				,os.ObjektType
				,os.object_executions_per_reporting_period
				,CAST((os.object_cpu_per_reporting_period / os.object_executions_per_reporting_period) / 1.0e3 AS DECIMAL(20, 6)) avg_cpu_ms
				,CAST((os.object_cpu_per_reporting_period / ts.total_cpu_per_reporting_period) * 100 AS DECIMAL(20, 6)) Total_cpu_andel
				,CAST((os.object_duration_per_reporting_period / os.object_executions_per_reporting_period) / 1.0e3 AS DECIMAL(20, 6)) Avg_duration_ms
				,CAST((os.object_duration_per_reporting_period / ts.total_duration_per_reporting_period) * 100 AS DECIMAL(20, 6)) Total_duration_andel
				,CAST((os.object_mem_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6))  avg_mem
				,CAST((os.object_mem_per_reporting_period / ts.total_mem_per_reporting_period) * 100 AS DECIMAL(20, 6)) Total_mem_andel
				,CAST((os.object_logical_io_reads_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) avg_logical_reads
				,CAST((os.object_logical_io_writes_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) avg_logical_writes
				,CAST((os.object_logical_io_reads_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) + CAST((os.object_logical_io_writes_per_reporting_period / os.object_executions_per_reporting_period) AS DECIMAL(20, 6)) avg_logical_io
				--		,(os.object_logical_io_reads_per_reporting_period + os.object_logical_io_writes_per_reporting_period) total_object_io
				--		,(ts.total_logical_io_reads_per_reporting_period + ts.total_logical_io_writes_per_reporting_period) total_io
				,CAST(((os.object_logical_io_reads_per_reporting_period + os.object_logical_io_writes_per_reporting_period) / (ts.total_logical_io_reads_per_reporting_period + ts.total_logical_io_writes_per_reporting_period)) * 100 AS DECIMAL(20, 6)) Total_io_andel
			FROM #ReportingPeriodObjectSums os
			INNER JOIN #ReportingPeriodTotalSums ts ON os.HourPeriod = ts.HourPeriod

			--Udtræk resultatet
			IF @DetailMode = 0
				BEGIN
				    SET @sql_select = N''
				    SET @sql_where = N''
				    SET @sql_where += N' AND objectname like '''+'%'+@StoredProcName+'%'+''' '

				    SET @sql_select = 
				    N'SELECT Rapporteringsperiode
						,ObjectName
						,ObjektType
						,object_executions_per_reporting_period
						,avg_cpu_ms
						,Total_cpu_andel
						,Avg_duration_ms
						,Total_duration_andel
						,avg_mem
						,Total_mem_andel
						,avg_logical_reads
						,avg_logical_writes
						,avg_logical_io
						,Total_io_andel
					FROM #ReportingPeriodResult
					WHERE 1=1'
					IF @StoredProcName is not null
					   BEGIN
						  SET @sql_select += @sql_where
					   END
					SET @sql_select += N' ORDER BY Rapporteringsperiode,avg_cpu_ms DESC'

--					PRINT @sql_select;

					EXEC sys.sp_executesql  @stmt = @sql_select
/*
					SELECT Rapporteringsperiode
						,ObjectName
						,ObjektType
						,object_executions_per_reporting_period
						,avg_cpu_ms
						,Total_cpu_andel
						,Avg_duration_ms
						,Total_duration_andel
						,avg_mem
						,Total_mem_andel
						,avg_logical_reads
						,avg_logical_writes
						,avg_logical_io
						,Total_io_andel
					FROM #ReportingPeriodResult
					--where Rapporteringsperiode = 2017120500
					--and ObjectName = 'dbo.UF_GetUnemploymentFundMembershipHistory'
					--skal kunne angives med parameter
					ORDER BY 1,5 DESC
*/
				END
			ELSE
				BEGIN
				    SET @sql_select = N''
				    SET @sql_where = N''
				    SET @sql_where += N' AND objectname like '''+'%'+@StoredProcName+'%'+''' '

				    SET @sql_select = 
				    N'SELECT DBName
						,ObjectName
						,query_id
						,plan_id
						,ObjektType
						,YEAR(interval_start_time)*10000 + MONTH(interval_start_time)*100+DAY(interval_start_time) DayPeriod
						,YEAR(interval_start_time)*1000000 + MONTH(interval_start_time)*10000+DAY(interval_start_time)*100+DATEPART(HH,interval_start_time) HourPeriod
						,runtime_stats_interval_id
						,count_executions
						,avg_cpu_time
						,cpu_per_interval
						,avg_duration
						,duration_per_interval
						,avg_query_max_used_memory
						,mem_per_interval
						,avg_logical_io_reads 
						,logical_io_reads_per_interval
						,avg_logical_io_writes
						,logical_io_writes_per_interval
						,avg_physical_io_reads 
						,physical_io_reads_per_interval
					FROM #periodexecutions
					WHERE 1=1'
					IF @StoredProcName is not null
					   BEGIN
						  SET @sql_select += @sql_where
					   END
					SET @sql_select += N' ORDER BY ObjectName,runtime_stats_interval_id,query_id,plan_id'

--					PRINT @sql_select;

					EXEC sys.sp_executesql  @stmt = @sql_select

/*
					SELECT DBName
						,ObjectName
						,query_id
						,plan_id
						,ObjektType
						,YEAR(interval_start_time)*10000 + MONTH(interval_start_time)*100+DAY(interval_start_time) DayPeriod
						,YEAR(interval_start_time)*1000000 + MONTH(interval_start_time)*10000+DAY(interval_start_time)*100+DATEPART(HH,interval_start_time) HourPeriod
						,runtime_stats_interval_id
						,count_executions
						,avg_cpu_time
						,cpu_per_interval
						,avg_duration
						,duration_per_interval
						,avg_query_max_used_memory
						,mem_per_interval
						,avg_logical_io_reads 
						,logical_io_reads_per_interval
						,avg_logical_io_writes
						,logical_io_writes_per_interval
						,avg_physical_io_reads 
						,physical_io_reads_per_interval
					FROM #periodexecutions
					--where ObjectName = 'dbo.UF_GetUnemploymentFundMembershipHistory'
					--skal kunne angives med parameter
					ORDER BY ObjectName,runtime_stats_interval_id,query_id,plan_id
*/
				END
		END
	--drop de temporære tabeller
	DROP TABLE IF EXISTS #periodexecutions;
	DROP TABLE IF EXISTS #ReportingPeriodTotalSums;
	DROP TABLE IF EXISTS #ReportingPeriodObjectSums;
	DROP TABLE IF EXISTS #periodexecutionsums;
	DROP TABLE IF EXISTS #ReportingPeriodResult;
	DROP TABLE IF EXISTS #ReportingPeriodTotalSumsWeek;
	DROP TABLE IF EXISTS #ReportingPeriodObjectSumsWeek;
	DROP TABLE IF EXISTS #periodexecutionsumsWeek;
	DROP TABLE IF EXISTS #ReportingPeriodResultWeek;
END
GO

PRINT 'PROCEDURE [info].[QS_ObjectExecutionStatistics] ALTERED'
GO

GRANT EXECUTE on [info].[QS_ObjectExecutionStatistics] to PUBLIC