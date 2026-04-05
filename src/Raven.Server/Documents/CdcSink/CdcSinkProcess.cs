using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.CdcSink.Stats;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands.CdcSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;


namespace Raven.Server.Documents.CdcSink;

public abstract class CdcSinkProcess : IDisposable, ILowMemoryHandler
{
    internal const string Tag = "CDC Sink";

    private CancellationTokenSource _cts;
    private PoolOfThreads.LongRunningWork _longRunningWork;

    private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();

    protected readonly RavenLogger Logger;

    private CdcSinkStatsAggregator _lastStats;

    private readonly ConcurrentQueue<CdcSinkStatsAggregator> _lastCdcSinkStats = new();

    protected readonly CdcSinkDocumentProcessor DocumentProcessor;

    protected CdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        Logger = database.Loggers.GetLogger(GetType());
        Database = database;
        Configuration = configuration;
        Name = Configuration.Name;
        Statistics = new CdcSinkProcessStatistics(Tag, Name, Database.NotificationCenter);
        DocumentProcessor = new CdcSinkDocumentProcessor(configuration) { Logger = Logger };
    }

    protected CancellationToken CancellationToken => _cts.Token;

    public DocumentDatabase Database { get; }

    /// <summary>
    /// Completed when the initial load phase finishes. Created at construction time
    /// so tests can await it without racing against the process start.
    /// </summary>
    internal Task InitialLoadCompleted => _initialLoadTcs.Task;
    protected readonly TaskCompletionSource _initialLoadTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Raised each time RunInternalAsync fails with an exception (before entering fallback mode).
    /// Useful for tests that need to observe process errors without polling the notification center.
    /// </summary>
    internal event Action<Exception> ProcessError;

    /// <summary>
    /// The last exception thrown by RunInternalAsync (null if the process hasn't failed).
    /// </summary>
    internal Exception LastProcessException { get; private set; }

    public CdcSinkProcessStatistics Statistics { get; }

    public long TaskId => Configuration.TaskId;

    public string Name { get; }

    public CdcSinkConfiguration Configuration { get; }

    public TimeSpan? FallbackTime { get; protected set; }

    public OngoingTaskConnectionStatus GetConnectionStatus()
    {
        if (Configuration.Disabled || CancellationToken.IsCancellationRequested)
            return OngoingTaskConnectionStatus.NotActive;

        if (FallbackTime != null)
            return OngoingTaskConnectionStatus.Reconnect;

        if (Statistics.WasLatestConsumeSuccessful || Statistics.ConsumeErrors == 0)
            return OngoingTaskConnectionStatus.Active;

        return OngoingTaskConnectionStatus.NotActive;
    }

    public static CdcSinkProcessState GetProcessState(DocumentDatabase database, string configurationName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var stateBlittable = database.ServerStore.Cluster.Read(context,
                CdcSinkProcessState.GenerateItemName(database.Name, configurationName));

            if (stateBlittable != null)
            {
                return JsonDeserializationClient.CdcSinkProcessState(stateBlittable);
            }

            return new CdcSinkProcessState();
        }
    }

    protected void UpdateProcessState(CdcSinkProcessState state)
    {
        var command = new UpdateCdcSinkProcessStateCommand(Database.Name, state, Database.ServerStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());

        var sendToLeaderTask = Database.ServerStore.SendToLeaderAsync(command);

        sendToLeaderTask.Wait(CancellationToken);
        var (etag, _) = sendToLeaderTask.Result;

        Database.RachisLogIndexNotifications.WaitForIndexNotification(etag, Database.ServerStore.Engine.OperationTimeout).Wait(CancellationToken);
    }

    /// <summary>
    /// The actual work for the CDC setup, initial load, streaming.
    /// Throwing exits the current attempt; the retry loop in <see cref="Run"/>
    /// will enter fallback mode and call this again after the backoff period.
    /// <see cref="OperationCanceledException"/> exits the process cleanly.
    /// </summary>
    protected abstract Task RunInternalAsync(CancellationToken ct);

    private async Task RunWithRetryAsync(CancellationToken ct)
    {
        while (true)
        {
            try
            {
                if (ct.IsCancellationRequested)
                    return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            if (FallbackTime != null)
            {
                if (ct.WaitHandle.WaitOne(FallbackTime.Value))
                    return;

                FallbackTime = null;
            }

            try
            {
                await RunInternalAsync(ct);
            }
            catch (OperationCanceledException)
            {
                _initialLoadTcs.TrySetCanceled();
                return;
            }
            catch (Exception e)
            {
                //  _initialLoadTcs.TrySetException(e);
                // Intentionally DOES NOT fault the _initialLoadTcs here — the process will retry after fallback.
                // If we set TrySetException, a subsequent successful retry can never signal completion.

                LastProcessException = e;
                ProcessError?.Invoke(e);

                if (Logger.IsErrorEnabled)
                    Logger.Error($"[{Name}] CDC Sink process failed.", e);
                var alert = AlertRaised.Create(
                    Database.Name, Tag,
                    $"[{Name}] CDC Sink process failed: {e.Message}",
                    AlertReason.CdcSink_Error,
                    NotificationSeverity.Error,
                    key: $"{Tag}/{Name}",
                    details: new ExceptionDetails(e));

                Database.NotificationCenter.Add(alert);

                EnterFallbackMode();
            }
        }
    }

    protected void AddPerformanceStats(CdcSinkStatsAggregator stats)
    {
        _lastStats = stats;

        _lastCdcSinkStats.Enqueue(stats);

        while (_lastCdcSinkStats.Count > 25)
            _lastCdcSinkStats.TryDequeue(out _);
    }

    public void Start()
    {
        if (_longRunningWork != null)
            return;

        if (Configuration.Disabled)
            return;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown);

        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

        var threadName = $"{Tag} process: {Name}";
        _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
        {
            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                NativeMemory.EnsureRegistered();
                AsyncHelpers.RunSync(() => RunWithRetryAsync(_cts.Token));
            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to run CDC Sink {Name}", e);
            }
        }, null, ThreadNames.ForCdcSinkProcess(threadName, Tag, Name));

        if (Logger.IsInfoEnabled)
            Logger.Info($"Starting {Tag} process: '{Name}'.");
    }

    public static TestCdcSinkScriptResult TestScript(TestCdcSinkScript testScript, DocumentsOperationContext context, DocumentDatabase database)
    {
        testScript.Configuration.Initialize(connectionString: null);

        testScript.Configuration.TestMode = true;

        if (testScript.Configuration.Validate(out List<string> errors) == false)
        {
            throw new InvalidOperationException(
                $"Invalid CDC Sink configuration for '{testScript.Configuration.Name}'. " +
                $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.");
        }

        if (testScript.Configuration.Tables.Count != 1)
        {
            throw new InvalidOperationException(
                $"Invalid number of tables. You have provided {testScript.Configuration.Tables.Count} " +
                "while CDC Sink test expects to get exactly 1 table");
        }

        if (string.IsNullOrEmpty(testScript.Message))
            throw new InvalidOperationException("Sample message in JSON format must be provided");

        using var messageDoc = context.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(testScript.Message)), "cdc-sink-test-message");

        using (context.OpenWriteTransaction())
        {
            // Build the same combined patch request used in production.
            var docProcessor = new CdcSinkDocumentProcessor(testScript.Configuration);
            var patchRequest = docProcessor.CombinedPatchRequest
                ?? throw new InvalidOperationException("The table has no patch script configured.");

            var table = testScript.Configuration.Tables[0];

            // Wrap the message in the same $rows format that production uses,
            // so the dispatch script and per-table functions work identically.
            var rowsArgs = new DynamicJsonValue
            {
                ["rows"] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        ["table"] = table.SourceTableName,
                        ["row"] = new DynamicJsonValue(messageDoc)
                    }
                }
            };
            using var argsBlittable = context.ReadObject(rowsArgs, "cdc-test-args");

            var command = new TestCdcMessageCommand(context, patchRequest, argsBlittable);

            command.Execute(context, null);

            return new TestCdcSinkScriptResult
            {
                DebugOutput = command.DebugOutput,
                Actions = command.DebugActions
            };
        }
    }

    public void Stop(string reason)
    {
        if (_longRunningWork == null)
            return;

        string msg = $"Stopping {Tag} process: '{Name}'. Reason: {reason}";

        if (Logger.IsInfoEnabled)
        {
            Logger.Info(msg);
        }

        _cts.Cancel();

        var longRunningWork = _longRunningWork;
        _longRunningWork = null;

        if (longRunningWork != PoolOfThreads.LongRunningWork.Current)
            longRunningWork.Join(int.MaxValue);

        _cts.Dispose();
    }

    protected void EnterFallbackMode()
    {
        if (Statistics.LastConsumeErrorTime is DateTime lastErrorTime)
        {
            var secondsSinceLastError =
                (Database.Time.GetUtcNow() - lastErrorTime).TotalSeconds;

            var maxSeconds = Database.Configuration.CdcSink.MaxFallbackTime.AsTimeSpan.TotalSeconds;
            FallbackTime = TimeSpan.FromSeconds(Math.Min(maxSeconds, Math.Max(5, secondsSinceLastError * 2)));
        }
        else
        {
            FallbackTime = TimeSpan.FromSeconds(5);
        }
    }

    protected async Task<(string Checkpoint, int Rows)> SubmitBatch(List<CdcSinkDocumentOp> ops, string checkpoint = null,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates = null)
    {
        // Compact nulls — streaming paths add null entries for rows from unconfigured tables
        int pos = 0;
        for (int i = 0; i < ops.Count; i++)
        {
            ops[pos] = ops[i];
            pos += (ops[i] != null).ToInt32();
        }
        ops.RemoveRange(pos, ops.Count - pos);

        // We *intentionally* submit the command here, even if we have no entries
        // to update the checkpoint and table load state in a timely manner

        var command = new Commands.CdcSinkBatchCommand(
            Database, ops, Configuration.Name, checkpoint,
            tableLoadUpdates: tableLoadUpdates,
            patchRequest: DocumentProcessor.CombinedPatchRequest,
            statsScope: null, statistics: Statistics, logger: Logger);

        var start = Stopwatch.GetTimestamp();
        await Database.TxMerger.Enqueue(command);

        if (Logger.IsDebugEnabled)
            Logger.Debug($"[{Name}] SubmitBatch: {command.ProcessedSuccessfully} ops persisted in {Stopwatch.GetElapsedTime(start).TotalMilliseconds:#,#} ms, checkpoint={checkpoint ?? "(none)"}");

        Database.CdcSinkLoader.OnBatchCompleted(Configuration.Name, Name, Statistics);
        return (checkpoint, ops.Count);
    }


    protected enum CdcEventType { Upsert, Delete, TransactionCommit }

    protected readonly record struct CdcEvent(CdcEventType Type, CdcSinkDocumentOp Op, string Checkpoint);

    /// <summary>
    /// Returns an async stream of CDC events from the source database.
    /// Each subclass converts provider-specific events into <see cref="CdcEvent"/>:
    /// <list type="bullet">
    /// <item>Insert/Update → <see cref="CdcEventType.Upsert"/> with the decoded op</item>
    /// <item>Delete → <see cref="CdcEventType.Delete"/> with the decoded op</item>
    /// <item>Transaction boundary → <see cref="CdcEventType.TransactionCommit"/> with checkpoint string</item>
    /// </list>
    /// </summary>
    protected abstract IAsyncEnumerable<CdcEvent> GetCdcEvents(CancellationToken ct);

    /// <summary>
    /// Called after each batch is submitted to the TxMerger. Subclasses can override to
    /// perform provider-specific acknowledgment (e.g. PostgreSQL WAL status update).
    /// </summary>
    protected virtual Task OnBatchFlushed(string checkpoint, int rows) => Task.CompletedTask;

    /// <summary>
    /// Common streaming loop shared by all providers. Uses MoveNextAsync + Task.WhenAny
    /// to overlap event reading with batch processing — when the TxMerger finishes a batch
    /// while we're waiting for the next event, we immediately flush any accumulated ops
    /// without waiting for another event to arrive.
    /// </summary>
    /// <summary>
    /// The current JSON parsing context for CDC streaming. Subclasses use this in
    /// <see cref="GetCdcEvents"/> when decoding rows. Rotated on each batch flush
    /// so that blittable objects from the previous batch stay alive until the TxMerger
    /// finishes writing them.
    /// </summary>
    protected DocumentsOperationContext StreamingJsonContext { get; private set; }

    protected async Task ProcessCdcStream(CancellationToken ct)
    {
        var batch = new List<CdcSinkDocumentOp>();
        var pending = new List<CdcSinkDocumentOp>();
        var emptyTask = Task.FromResult<(string, int)>((null, 0));
        Task<(string, int)> lastBatch = emptyTask;
        string lastCheckpoint = null;

        // Context rotation: blittable objects created by ProcessRow reference the context's
        // memory. We allocate a new context per batch so the previous one stays alive until
        // the TxMerger finishes writing it.
        IDisposable previousCtx = null;
        var currentCtx = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx);
        StreamingJsonContext = ctx;

        async Task FlushBatch()
        {
            (string completedCheckpoint, int rows) = await lastBatch;
            if (completedCheckpoint is not null)
                await OnBatchFlushed(completedCheckpoint, rows);

            // Rotate context: previous batch's blittables stay alive in previousCtx
            // until the next FlushBatch call disposes it after awaiting lastBatch.
            // Safe to dispose the context, since we just awaited on the previous batch's completion that used it
            previousCtx?.Dispose();
            previousCtx = currentCtx;
            currentCtx = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out ctx);
            StreamingJsonContext = ctx;

            if (batch.Count is 0)
            {
                // drop  reference to the last batch, in case it holds any resources / memory
                lastBatch = emptyTask;
                return;
            }

            lastBatch = SubmitBatch(batch, lastCheckpoint);
            batch = [];
        }

        try
        {
            await using var enumerator = GetCdcEvents(ct).GetAsyncEnumerator(ct);
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var moveNext = enumerator.MoveNextAsync();

                bool moveNextResult = false;

                if (moveNext.IsCompleted == false)
                {
                    var moveTask = moveNext.AsTask();

                    // Race: wait for either the next event or the previous batch to complete.
                    // This allows flushing accumulated ops while the source is idle or slow.
                    await Task.WhenAny(moveTask, lastBatch);

                    if (lastBatch.IsCompleted || ShouldFlushBatch(batch.Count))
                        await FlushBatch();

                    moveNextResult = await moveTask;
                }
                else
                {
                    moveNextResult = await moveNext;
                }
                
                if (moveNextResult is false)
                    break; // enumerator is completed (cancelled, probably)

                var evt = enumerator.Current;

                switch (evt.Type)
                {
                    case CdcEventType.Upsert or CdcEventType.Delete:
                        pending.Add(evt.Op);
                        break;

                    case CdcEventType.TransactionCommit:
                        batch.AddRange(pending);
                        pending.Clear();
                        lastCheckpoint = evt.Checkpoint;
                        break;
                }
            }

            // Stream ended — flush remaining ops, then wait for the final batch to complete
            await FlushBatch();
            (string finalCheckpoint, int rows) = await lastBatch;
            if (finalCheckpoint is not null) // force a flush, ensure the last LSN is acknowledged for PostgreSQL and similar providers.
                await OnBatchFlushed(finalCheckpoint, Database.Configuration.CdcSink.MaxBatchSize + 1); 
        }
        finally
        {
            previousCtx?.Dispose();
            currentCtx?.Dispose();
            StreamingJsonContext = null;
        }
    }

    /// <summary>
    /// Returns the default schema name for this database provider (e.g. "public" for PostgreSQL,
    /// "dbo" for SQL Server, database name for MySQL).
    /// </summary>
    protected abstract string GetDefaultSchema();

    /// <summary>
    /// Opens a database connection for the initial load phase. Called once before all tables
    /// are loaded. The connection is passed to <see cref="ReadOneBatch"/> and disposed after
    /// the load completes.
    /// </summary>
    protected abstract Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct);


    /// <summary>
    /// Whether this provider uses SELECT TOP(N) (SQL Server) vs LIMIT N (Postgres/MySQL).
    /// </summary>
    protected virtual bool UsesTopN => false;

    private string BuildBatchQuery(
        CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns,
        string[] lastKeys, int maxBatchSize)
    {
        var table = $"{CommandBuilder.QuoteIdentifier(tableInfo.Schema)}.{CommandBuilder.QuoteIdentifier(tableInfo.TableName)}";
        var pkCols = string.Join(", ", pkColumns.Select(c => CommandBuilder.QuoteIdentifier(c)));
        var where = lastKeys != null
            ? $" WHERE ({pkCols}) > ({string.Join(", ", pkColumns.Select((_, i) => $"@k{i}"))})"
            : "";

        return UsesTopN
            ? $"SELECT TOP ({maxBatchSize}) * FROM {table} {where} ORDER BY {pkCols}"
            : $"SELECT * FROM {table} {where} ORDER BY {pkCols} LIMIT {maxBatchSize}";
    }

    /// <summary>
    /// Binds keyset pagination parameters to the command. Postgres and SQL Server
    /// convert string keys to typed values; MySQL passes raw strings.
    /// Only called when <paramref name="lastKeys"/> is not null.
    /// </summary>
    protected abstract Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct);

    /// <summary>
    /// Converts a single column value from the initial load DbDataReader to a
    /// normalized CLR type for the document processor pipeline.
    /// </summary>
    protected abstract object ConvertInitialLoadValue(DbDataReader reader, int ordinal);

    private async Task<(List<CdcSinkDocumentOp> Ops, string[] LastKeys, IDisposable Context)> ReadOneBatch(
        DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns,
        string[] lastKeys, int maxBatchSize, CancellationToken ct)
    {
        var query = BuildBatchQuery(tableInfo, pkColumns, lastKeys, maxBatchSize);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = query;

        if (lastKeys != null)
            await BindKeysetParameters(cmd, tableInfo, pkColumns, lastKeys, ct);

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        // Context must outlive this method — blittable objects created by ProcessRow
        // (for CdcColumnType.Json columns) reference the context's memory. The caller
        // is responsible for disposing it after the batch has been processed.
        var ctxHolder = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingCtx);

        var ops = new List<CdcSinkDocumentOp>();

        while (await reader.ReadAsync(ct))
        {
            var data = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                data[reader.GetName(i)] = reader.IsDBNull(i) ? null : ConvertInitialLoadValue(reader, i);
            }

            var row = new CdcSinkRow
            {
                TableSchema = tableInfo.Schema,
                TableName = tableInfo.TableName,
                Operation = CdcSinkOperation.Upsert,
                Data = data,
            };

            ops.Add(DocumentProcessor.ProcessRow(row, jsonParsingCtx));
        }

        // Extract last keys from the last non-null op's RawData for keyset pagination resume.
        // Ops can contain nulls from DocumentProcessor.ProcessRow for unconfigured tables.
        string[] newLastKeys = null;
        for (int j = ops.Count - 1; j >= 0; j--)
        {
            if (ops[j]?.RawData == null)
                continue;
            var lastRowData = ops[j].RawData;
            newLastKeys = new string[pkColumns.Count];
            for (int i = 0; i < pkColumns.Count; i++)
                newLastKeys[i] = lastRowData.TryGetValue(pkColumns[i], out var v) ? v?.ToString() ?? "" : "";
            break;
        }

        return (ops, newLastKeys, ctxHolder);
    }


    protected async Task HandleInitialLoad(CancellationToken ct)
    {
        CdcSinkTaskState state;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            state = LoadState(context);
        }

        var allTables = Configuration.CollectAllTablesFlat(GetDefaultSchema());

        if (Configuration.SkipInitialLoad)
        {
            var updates = new Dictionary<string, CdcSinkTableLoadState>();
            foreach (var tableInfo in allTables)
            {
                var tableKey = tableInfo.FullName;
                if (state.Tables.TryGetValue(tableKey, out var ts) && ts.InitialLoadCompleted)
                    continue;
                updates[tableKey] = new CdcSinkTableLoadState { InitialLoadCompleted = true };
            }

            if (updates.Count > 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] SkipInitialLoad is set — marking {updates.Count} table(s) as complete without scanning.");
                await SubmitBatch([], tableLoadUpdates: updates);
            }

            return;
        }

        // Single connection for the entire initial load — reused across all tables
        await using var conn = await OpenInitialLoadConnection(ct);

        foreach (var tableInfo in allTables)
        {
            var tableKey = tableInfo.FullName;

            if (state.Tables.TryGetValue(tableKey, out var tableState) && tableState.InitialLoadCompleted)
                continue;

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Starting initial load for table {tableInfo.FullName}");

            await ProcessTableInitialLoad(conn, tableInfo, tableKey, tableState, ct);

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Completed initial load for table {tableInfo.FullName}");
        }
    }

    private async Task ProcessTableInitialLoad(
        DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, string tableKey,
        CdcSinkTableLoadState resumeState, CancellationToken ct)
    {
        var pkColumns = tableInfo.PrimaryKeyColumns;
        var maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize;

        string[] lastKeys = null;
        if (resumeState?.LastKeyValues != null && resumeState.LastKeyValues.Count == pkColumns.Count)
        {
            lastKeys = new string[resumeState.LastKeyValues.Count];
            for (int i = 0; i < resumeState.LastKeyValues.Count; i++)
                lastKeys[i] = resumeState.LastKeyValues[i];
        }

        var lastBatch = Task.CompletedTask;
        IDisposable previousBatchCtx = null;
        IDisposable currentBatchCtx = null;

        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var (ops, newLastKeys, batchCtx) = await ReadOneBatch(conn, tableInfo, pkColumns, lastKeys, maxBatchSize, ct);
                currentBatchCtx = batchCtx;

                if (ops.Count == 0)
                {
                    currentBatchCtx?.Dispose();
                    currentBatchCtx = null;

                    await lastBatch;
                    previousBatchCtx?.Dispose();
                    previousBatchCtx = null;

                    var finalUpdate = new Dictionary<string, CdcSinkTableLoadState>
                    {
                        [tableKey] = new CdcSinkTableLoadState { InitialLoadCompleted = true }
                    };
                    await SubmitBatch(new List<CdcSinkDocumentOp>(), tableLoadUpdates: finalUpdate);
                    return;
                }

                await lastBatch;
                previousBatchCtx?.Dispose();

                var tableLoadUpdate = new Dictionary<string, CdcSinkTableLoadState>
                {
                    [tableKey] = new CdcSinkTableLoadState { LastKeyValues = new List<string>(newLastKeys) }
                };

                lastBatch = SubmitBatch(ops, tableLoadUpdates: tableLoadUpdate);
                previousBatchCtx = currentBatchCtx;
                currentBatchCtx = null;
                lastKeys = newLastKeys;
            }
        }
        finally
        {
            previousBatchCtx?.Dispose();
            currentBatchCtx?.Dispose();
        }
    }

    public CdcSinkPerformanceStats[] GetPerformanceStats()
    {
        var lastStats = _lastStats;
        List<CdcSinkPerformanceStats> result = [];

        foreach (var stats in _lastCdcSinkStats)
        {
            result.Add(stats == lastStats
                ? stats.ToPerformanceLiveStatsWithDetails()
                : stats.ToPerformanceStats());
        }

        return result.ToArray();
    }

    public CdcSinkStatsAggregator GetLatestPerformanceStats()
    {
        return _lastStats;
    }

    /// <summary>
    /// Lightweight batch-size check for the streaming loops in derived classes.
    /// Returns false when the batch should be flushed (size limit, low memory, or CPU credits).
    /// </summary>
    protected bool ShouldFlushBatch(int batchSize)
    {
        if (batchSize >= Database.Configuration.CdcSink.MaxBatchSize)
            return true;

        // we are always called with a full transaction, so no point in 
        // trying to fill a whole batch in low mem state
        return _lowMemoryFlag.IsRaised() ||
            Database.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised();
    }


    protected CdcSinkTaskState LoadState(DocumentsOperationContext context)
    {
        var stateDocId = CdcSinkTaskState.GetDocumentId(Configuration.Name);
        var doc = Database.DocumentsStorage.Get(context, stateDocId);

        if (doc == null)
            return new CdcSinkTaskState { ConfigurationName = Configuration.Name };

        return JsonDeserializationServer.CdcSinkTaskState(doc.Data);
    }

    public virtual void Dispose()
    {
        var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

        exceptionAggregator.Execute(() => Stop("Dispose"));

        exceptionAggregator.Execute(() => _cts.Dispose());

        exceptionAggregator.Execute(() => CommandBuilder.Dispose());

        exceptionAggregator.ThrowIfNeeded();
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        _lowMemoryFlag.Raise();
    }

    public void LowMemoryOver()
    {
        _lowMemoryFlag.Lower();
    }

    internal static void AddParameter(DbCommand cmd, string name, object value)
    {
        var param = cmd.CreateParameter();
        param.ParameterName = name;
        param.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(param);
    }

    /// <summary>
    /// Provider-specific command builder for safe identifier quoting via
    /// <see cref="DbCommandBuilder.QuoteIdentifier"/>. Disposed with the process.
    /// </summary>
    protected abstract DbCommandBuilder CommandBuilder { get; }
}
