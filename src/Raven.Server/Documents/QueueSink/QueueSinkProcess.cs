
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink.Commands;
using Raven.Server.Documents.QueueSink.Stats;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Documents.QueueSink.Test;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.QueueSink;

public abstract class QueueSinkProcess : IDisposable, ILowMemoryHandler
{
    internal const string KafkaTag = "Kafka Sink"; 
    internal const string RabbitMqTag = "RabbitMQ Sink";

    private const int MinBatchSize = 16;

    private CancellationTokenSource _cts;
    private PoolOfThreads.LongRunningWork _longRunningWork;

    private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

    private NativeMemory.ThreadStats _threadAllocations;
    private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
    private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

    protected readonly RavenLogger Logger;

    private int _statsId;
    private QueueSinkStatsAggregator _lastStats;

    private readonly ConcurrentQueue<QueueSinkStatsAggregator> _lastQueueSinkStats = new();

    private IQueueSinkConsumer _consumer;

    protected QueueSinkProcess(QueueSinkConfiguration configuration, QueueSinkScript script,
        DocumentDatabase database, string tag)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        Logger = RavenLogManager.Instance.GetLoggerForDatabase(GetType(), database);
        Database = database;
        Configuration = configuration;
        Script = script;
        Tag = tag;
        Name = $"{Configuration.Name}/{Script.Name}";
        Statistics = new QueueSinkProcessStatistics(Tag, Name, Database.NotificationCenter);
    }

    public static QueueSinkProcess CreateInstance(QueueSinkScript script, QueueSinkConfiguration configuration,
        DocumentDatabase database)
    {
        switch (configuration.BrokerType)
        {
            case QueueBrokerType.Kafka:
                return new KafkaQueueSink(configuration, script, database, KafkaTag);
            case QueueBrokerType.RabbitMq:
                return new RabbitMqQueueSink(configuration, script, database, RabbitMqTag);
            default:
                throw new NotSupportedException($"Unknown broker type: {configuration.BrokerType}");
        }
    }

    protected CancellationToken CancellationToken => _cts.Token;

    protected string GroupId => $"{Database.DatabaseGroupId}/{Name}";

    public DocumentDatabase Database { get; }

    public QueueSinkProcessStatistics Statistics { get; }

    public long TaskId => Configuration.TaskId;

    public string Tag { get; }

    public string Name { get; }

    public QueueSinkConfiguration Configuration { get; }

    public QueueSinkScript Script { get; }

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

    public static QueueSinkProcessState GetProcessState(DocumentDatabase database, string configurationName,
        string scriptName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var stateBlittable = database.ServerStore.Cluster.Read(context,
                QueueSinkProcessState.GenerateItemName(database.Name, configurationName, scriptName));

            if (stateBlittable != null)
            {
                return JsonDeserializationClient.QueueSinkProcessState(stateBlittable);
            }

            return new QueueSinkProcessState();
        }
    }

    protected void UpdateProcessState(QueueSinkProcessState state)
    {
        var command = new UpdateQueueSinkProcessStateCommand(Database.Name, state, Database.ServerStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());

        var sendToLeaderTask = Database.ServerStore.SendToLeaderAsync(command);

        sendToLeaderTask.Wait(CancellationToken);
        var (etag, _) = sendToLeaderTask.Result;

        Database.RachisLogIndexNotifications.WaitForIndexNotification(etag, Database.ServerStore.Engine.OperationTimeout).Wait(CancellationToken);
    }

    private void Run()
    {
        while (true)
        {
            using var _ = Database.PreventFromUnloadingByIdleOperations();
            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            if (FallbackTime != null)
            {
                if (CancellationToken.WaitHandle.WaitOne(FallbackTime.Value))
                    return;

                FallbackTime = null;
            }

            EnsureThreadAllocationStats();

            try
            {
                if (_consumer == null)
                {
                    try
                    {
                        _consumer = CreateConsumer();
                    }
                    catch (Exception e)
                    {
                        string msg = $"[{Name}] Failed to create queue consumer";

                        if (Logger.IsErrorEnabled)
                            Logger.Error(msg, e);

                        var key = $"{Tag}/{Name}";

                        var alert = AlertRaised.Create(Database.Name, Tag, msg, AlertType.QueueSink_ConsumerCreationError, NotificationSeverity.Error, key, new ExceptionDetails(e));

                        Database.NotificationCenter.Add(alert);

                        EnterFallbackMode();
                        continue;
                    }
                }

                var statsAggregator = new QueueSinkStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);

                using (Statistics.NewBatch())
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var stats = statsAggregator.CreateScope())
                {
                    var messages = new List<BlittableJsonReaderObject>();

                    using (var readScope = stats.For(QueueSinkBatchPhases.QueueReading, start: false))
                    {
                        var batchStarted = false;

                        while (true)
                        {
                            try
                            {
                                var message = batchStarted
                                    ? _consumer.Consume(TimeSpan.Zero)
                                    : _consumer.Consume(CancellationToken);

                                if (message is null)
                                    break;

                                if (batchStarted == false)
                                {
                                    statsAggregator.Start();
                                    stats.Start();
                                    readScope.Start();

                                    AddPerformanceStats(statsAggregator);
                                }

                                batchStarted = true;

                                var json = context.Sync.ReadForMemory(new MemoryStream(message), "queue-message");

                                messages.Add(json);

                                readScope.RecordReadMessage();

                                if (CanContinueBatch(stats, messages.Count, context) == false)
                                    break;
                            }
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                string msg = "Failed to consume message.";

                                if (Logger.IsErrorEnabled)
                                    Logger.Error(msg, e);

                                readScope.RecordReadError();
                                Statistics.RecordConsumeError(e.Message);

                                if (batchStarted == false)
                                {
                                    // failed to consume any message, let's do the fallback then
                                    EnterFallbackMode();
                                }
                            }
                        }
                    }

                    var processedSuccessfully = 0;

                    try
                    {
                        using (var scriptProcessingScope = stats.For(QueueSinkBatchPhases.ScriptProcessing))
                        {
                            try
                            {
                                var command = new BatchQueueSinkScriptCommand(Script.Script, messages, scriptProcessingScope, Statistics, Logger);

                                Database.TxMerger.EnqueueSync(command);

                                processedSuccessfully = command.ProcessedSuccessfully;

                                _consumer.Commit();
                            }
                            catch (JavaScriptParseException e)
                            {
                                HandleScriptParseException(e);
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception e)
                    {
                        var message = $"{Tag} Exception in queue sink process '{Name}'";

                        if (Logger.IsErrorEnabled)
                            Logger.Error(message, e);
                    }

                    statsAggregator.Complete();
                    
                    if (processedSuccessfully > 0)
                    {
                        Statistics.ConsumeSuccess(processedSuccessfully);

                        try
                        {
                            UpdateProcessState(new QueueSinkProcessState
                            {
                                ConfigurationName = Configuration.Name,
                                ScriptName = Script.Name,
                                NodeTag = Database.ServerStore.NodeTag
                            });

                            Database.QueueSinkLoader.OnBatchCompleted(Configuration.Name, Script.Name, Statistics);
                        }
                        catch (Exception e)
                        {
                            if (CancellationToken.IsCancellationRequested == false)
                            {
                                if (Logger.IsErrorEnabled)
                                    Logger.Error($"{Tag} Failed to update state of queue sink process '{Name}'", e);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                var msg = $"Unexpected error in {Tag} process: '{Name}'";

                if (Logger.IsErrorEnabled)
                {
                    Logger.Error(msg, e);
                }
            }
            finally
            {
                _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
            }
        }
    }

    private void AddPerformanceStats(QueueSinkStatsAggregator stats)
    {
        _lastStats = stats;

        _lastQueueSinkStats.Enqueue(stats);

        while (_lastQueueSinkStats.Count > 25)
            _lastQueueSinkStats.TryDequeue(out _);
    }

    protected abstract IQueueSinkConsumer CreateConsumer();

    public void Start()
    {
        if (_longRunningWork != null)
            return;

        if (Script.Disabled || Configuration.Disabled)
            return;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown);

        var threadName = $"{Tag} process: {Name}";
        _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
        {
            try
            {
                // This has lower priority than request processing, so we let the OS
                // schedule this appropriately
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                NativeMemory.EnsureRegistered();
                Run();
            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to run Queue Sink {Name}", e);
            }
        }, null, ThreadNames.ForQueueSinkProcess(threadName, Tag, Name));

        if (Logger.IsInfoEnabled)
            Logger.Info($"Starting {Tag} process: '{Name}'.");

    }

    public static TestQueueSinkScriptResult TestScript(TestQueueSinkScript testScript, DocumentsOperationContext context, DocumentDatabase database)
    {
        testScript.Configuration.Initialize(connectionString: null);

        testScript.Configuration.TestMode = true;

        if (testScript.Configuration.Validate(out List<string> errors) == false)
        {
            throw new InvalidOperationException(
                $"Invalid Queue Sink configuration for '{testScript.Configuration.Name}'. " +
                $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.");
        }

        if (testScript.Configuration.Scripts.Count != 1)
        {
            throw new InvalidOperationException(
                $"Invalid number of scripts. You have provided {testScript.Configuration.Scripts.Count} " +
                "while Queue Sink test expects to get exactly 1 script");
        }

        if (string.IsNullOrEmpty(testScript.Message))
            throw new InvalidOperationException("Sample message in JSON format must be provided");

        using var messageDoc = context.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(testScript.Message)), "queue-sink-test-message");

        using (context.OpenWriteTransaction())
        {
            var script = new PatchRequest(testScript.Configuration.Scripts[0].Script, PatchRequestType.QueueSink);

            var command = new TestQueueMessageCommand(context, script, messageDoc);

            command.Execute(context, null);

            return new TestQueueSinkScriptResult
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

        if (longRunningWork != PoolOfThreads.LongRunningWork.Current) // prevent a deadlock
            longRunningWork.Join(int.MaxValue);

        _consumer?.Dispose();
        _consumer = null;
    }

    private void HandleScriptParseException(Exception e)
    {
        var message = $"[{Name}] Could not parse script. Stopping Queue Sink process.";

        if (Logger.IsInfoEnabled)
            Logger.Info(message, e);

        var key = $"{Tag}/{Name}";
        var details = new QueueSinkErrorsDetails();

        details.Errors.Enqueue(new QueueSinkErrorInfo(e.ToString()));

        var alert = AlertRaised.Create(
            Database.Name,
            Tag,
            message,
            AlertType.QueueSink_ScriptError,
            NotificationSeverity.Error,
            key: key,
            details: details);

        Database.NotificationCenter.Add(alert);

        Stop(message);
    }

    private void EnterFallbackMode()
    {
        if (Statistics.LastConsumeErrorTime == null)
            FallbackTime = TimeSpan.FromSeconds(5);
        else
        {
            // double the fallback time (but don't cross QueueSink.MaxFallbackTimeInSec)
            var secondsSinceLastError =
                (Database.Time.GetUtcNow() - Statistics.LastConsumeErrorTime.Value).TotalSeconds;

            FallbackTime = TimeSpan.FromSeconds(Math.Min(
                Database.Configuration.QueueSink
                    .MaxFallbackTime.AsTimeSpan.TotalSeconds,
                Math.Max(5, secondsSinceLastError * 2)));
        }
    }

    public QueueSinkPerformanceStats[] GetPerformanceStats()
    {
        var lastStats = _lastStats;

        return _lastQueueSinkStats
            .Select(x => x == lastStats ? x.ToPerformanceLiveStatsWithDetails() : x.ToPerformanceStats())
            .ToArray();
    }

    public QueueSinkStatsAggregator GetLatestPerformanceStats()
    {
        return _lastStats;
    }

    private bool CanContinueBatch(QueueSinkStatsScope stats, int batchSize, DocumentsOperationContext ctx)
    {
        if (Database.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
        {
            var reason = $"Stopping the batch after {stats.Duration} because the CPU credits balance is almost completely used";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);

            return false;
        }

        if (_lowMemoryFlag.IsRaised() && batchSize >= MinBatchSize)
        {
            var reason = $"The batch was stopped after processing {batchSize:#,#;;0} items because of low memory";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);
            return false;
        }

        var totalAllocated = new Size(_threadAllocations.TotalAllocated, SizeUnit.Bytes);
        _threadAllocations.CurrentlyAllocatedForProcessing = totalAllocated.GetValue(SizeUnit.Bytes);

        stats.RecordCurrentlyAllocated(totalAllocated.GetValue(SizeUnit.Bytes) + GC.GetAllocatedBytesForCurrentThread());

        if (totalAllocated > _currentMaximumAllowedMemory)
        {
            if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                    totalAllocated,
                    Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Database.ServerStore.Server.MetricCacher, Logger, out var memoryUsage) == false)
            {
                var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {totalAllocated}.";
                if (memoryUsage != null)
                {
                    reason += " Current memory usage: " +
                               $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                               $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";
                }

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordPullCompleteReason(reason);

                ctx.DoNotReuse = true;

                return false;
            }
        }

        var maxBatchSize = Database.Configuration.QueueSink.MaxBatchSize;

        if (maxBatchSize != null && batchSize >= maxBatchSize)
        {
            var reason = $"Stopping the batch because maximum batch size limit was reached ({batchSize})";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);

            return false;
        }

        return true;
    }

    protected void EnsureThreadAllocationStats()
    {
        _threadAllocations = NativeMemory.CurrentThreadStats;
    }

    public void Dispose()
    {
        if (CancellationToken.IsCancellationRequested)
            return;

        var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

        exceptionAggregator.Execute(() => Stop("Dispose"));

        exceptionAggregator.Execute(() => _cts.Dispose());
        exceptionAggregator.Execute(() => _consumer?.Dispose());

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
