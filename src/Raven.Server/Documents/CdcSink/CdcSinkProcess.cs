using System;
using System.Collections.Concurrent;
using System.Data.Common;
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
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands.CdcSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.CdcSink;

public abstract class CdcSinkProcess : IDisposable, ILowMemoryHandler
{
    internal const string Tag = "CDC Sink";

    private const int MinBatchSize = 16;

    private CancellationTokenSource _cts;
    private PoolOfThreads.LongRunningWork _longRunningWork;

    private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

    private NativeMemory.ThreadStats _threadAllocations;
    private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
    private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

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
    /// The actual work for a single iteration: setup, initial load, streaming.
    /// Throwing exits the current attempt; the retry loop in <see cref="Run"/>
    /// will enter fallback mode and call this again after the backoff period.
    /// <see cref="OperationCanceledException"/> exits the process cleanly.
    /// </summary>
    protected abstract Task RunInternalAsync(CancellationToken ct);

    private void Run()
    {
        AsyncHelpers.RunSync(() => RunWithRetryAsync(CancellationToken));
    }

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
                Run();
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
    }

    protected void EnterFallbackMode()
    {
        if (Statistics.LastConsumeErrorTime == null)
            FallbackTime = TimeSpan.FromSeconds(5);
        else
        {
            var secondsSinceLastError =
                (Database.Time.GetUtcNow() - Statistics.LastConsumeErrorTime.Value).TotalSeconds;

            FallbackTime = TimeSpan.FromSeconds(Math.Min(
                Database.Configuration.CdcSink
                    .MaxFallbackTime.AsTimeSpan.TotalSeconds,
                Math.Max(5, secondsSinceLastError * 2)));
        }
    }

    protected async Task SubmitBatch(List<CdcSinkDocumentOp> ops, string lastLsn = null,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates = null)
    {
        // Compact nulls — streaming paths add null entries for rows from unconfigured tables
        int pos = 0;
        for (int i = 0; i < ops.Count; i++)
        {
            ops[pos] = ops[i];
            pos+= (ops[i] != null).ToInt32();
        }
        ops.RemoveRange(pos, ops.Count - pos);

        var command = new Commands.CdcSinkBatchCommand(
            Database, ops, Configuration.Name, lastLsn,
            tableLoadUpdates: tableLoadUpdates,
            patchRequest: DocumentProcessor.CombinedPatchRequest,
            statsScope: null, statistics: Statistics, logger: Logger);

        await Database.TxMerger.Enqueue(command);

        Database.CdcSinkLoader.OnBatchCompleted(Configuration.Name, Name, Statistics);
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

    /// <summary>Opening quote character for SQL identifiers (e.g. '"' for Postgres, '[' for SQL Server, '`' for MySQL).</summary>
    protected abstract char StartQuote { get; }

    /// <summary>Closing quote character for SQL identifiers (e.g. '"' for Postgres, ']' for SQL Server, '`' for MySQL).</summary>
    protected abstract char EndQuote { get; }

    /// <summary>
    /// Whether this provider uses SELECT TOP(N) (SQL Server) vs LIMIT N (Postgres/MySQL).
    /// </summary>
    protected virtual bool UsesTopN => false;

    private string BuildBatchQuery(
        CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns,
        string[] lastKeys, int maxBatchSize)
    {
        var table = $"{StartQuote}{tableInfo.Schema}{EndQuote}.{StartQuote}{tableInfo.TableName}{EndQuote}";
        var pkCols = string.Join(", ", pkColumns.Select(c => $"{StartQuote}{c}{EndQuote}"));
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

            var op = DocumentProcessor.ProcessRow(row, jsonParsingCtx);
            if (op != null)
                ops.Add(op);
        }

        // Extract last keys from the last row's RawData for keyset pagination resume
        string[] newLastKeys = null;
        if (ops.Count > 0)
        {
            var lastRowData = ops[ops.Count - 1].RawData;
            newLastKeys = new string[pkColumns.Count];
            for (int i = 0; i < pkColumns.Count; i++)
                newLastKeys[i] = lastRowData.TryGetValue(pkColumns[i], out var v) ? v?.ToString() ?? "" : "";
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

        if (_lowMemoryFlag.IsRaised() && batchSize >= MinBatchSize)
            return true;

        if (Database.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
            return true;

        return false;
    }


    protected void EnsureThreadAllocationStats()
    {
        _threadAllocations = NativeMemory.CurrentThreadStats;
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

        exceptionAggregator.ThrowIfNeeded();
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        _lowMemoryFlag.Raise();
    }

    public void LowMemoryOver()
    {
        _lowMemoryFlag.Lower();
    }

    internal static void AddParameter(System.Data.Common.DbCommand cmd, string name, object value)
    {
        var param = cmd.CreateParameter();
        param.ParameterName = name;
        param.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(param);
    }
}
