using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
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
using Raven.Server.ServerWide.Commands.CdcSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
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

    protected CdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        Logger = database.Loggers.GetLogger(GetType());
        Database = database;
        Configuration = configuration;
        Name = Configuration.Name;
        Statistics = new CdcSinkProcessStatistics(Tag, Name, Database.NotificationCenter);
    }

    protected CancellationToken CancellationToken => _cts.Token;

    public DocumentDatabase Database { get; }

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

    public static CdcSinkProcessState GetProcessState(DocumentDatabase database, string configurationName, string scriptName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var stateBlittable = database.ServerStore.Cluster.Read(context,
                CdcSinkProcessState.GenerateItemName(database.Name, configurationName, scriptName));

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

    protected abstract void Run();

    private void AddPerformanceStats(CdcSinkStatsAggregator stats)
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
            var table = testScript.Configuration.Tables[0];

            // Generate the same combined-script format used in production:
            // per-table function receives $row as parameter, called with doc as `this`.
            var sb = new StringBuilder();
            sb.Append("function __cdc_test($row) {\n");
            sb.Append(table.Patch).Append("\n}\n\n");
            sb.Append("function execute(doc, args) {\n");
            sb.Append("  __cdc_test.call(doc, args);\n");
            sb.Append("  return doc;\n");
            sb.Append("}\n");

            // In test mode: the message IS the $row data, and the document is the message.
            // PatchDocumentCommand calls execute(doc, args) where doc = GetCurrentDocument()
            // and args = the second patch tuple (which we set to messageDoc below).
            var script = new PatchRequest(sb.ToString(), PatchRequestType.EtlBehaviorFunctions);

            var command = new TestCdcMessageCommand(context, script, messageDoc);

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

    public void Dispose()
    {
        if (CancellationToken.IsCancellationRequested)
            return;

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
}
