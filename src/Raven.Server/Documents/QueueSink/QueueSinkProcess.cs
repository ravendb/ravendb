
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
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink.Stats;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Documents.QueueSink.Test;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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

namespace Raven.Server.Documents.QueueSink;

public abstract class QueueSinkProcess : IDisposable, ILowMemoryHandler
{
    private const int MinBatchSize = 16;

    private CancellationTokenSource _cts;
    private PoolOfThreads.LongRunningWork _longRunningWork;

    private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

    private NativeMemory.ThreadStats _threadAllocations;
    private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
    private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

    private readonly Logger _logger;

    private int _statsId;
    private QueueSinkStatsAggregator _lastStats;

    private readonly ConcurrentQueue<QueueSinkStatsAggregator> _lastQueueSinkStats = new();

    private IQueueSinkConsumer _consumer;

    protected QueueSinkProcess(QueueSinkConfiguration configuration, QueueSinkScript script,
        DocumentDatabase database, string tag)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        _logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
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
                return new KafkaQueueSink(configuration, script, database, "Kafka Sink");
            case QueueBrokerType.RabbitMq:
                return new RabbitMqQueueSink(configuration, script, database, "RabbitMQ Sink");
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

    private void Run()
    {
        while (true)
        {
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

                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(msg, e);

                        var key = $"{Tag}/{Name}";

                        var alert = AlertRaised.Create(Database.Name, Tag, msg, AlertType.QueueSink_ConsumerCreationError, NotificationSeverity.Error, key, new ExceptionDetails(e));

                        Database.NotificationCenter.Add(alert);

                        EnterFallbackMode();
                        continue;
                    }
                }

                var statsAggregator = new QueueSinkStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);
                _lastStats = statsAggregator;

                AddPerformanceStats(statsAggregator);

                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var stats = statsAggregator.CreateScope())
                {
                    var messages = new List<BlittableJsonReaderObject>();

                    using (var pullScope = stats.For(QueueSinkBatchPhases.Pull, start: false))
                    {
                        while (true)
                        {
                            try
                            {
                                var message = messages.Count == 0
                                    ? _consumer.Consume(CancellationToken)
                                    : _consumer.Consume(TimeSpan.Zero);

                                if (message is null)
                                    break;

                                if (pullScope.IsRunning == false)
                                    pullScope.Start();

                                var json = context.Sync.ReadForMemory(new MemoryStream(message), "queue-message");

                                messages.Add(json);

                                pullScope.RecordPulledMessage();

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

                                if (_logger.IsOperationsEnabled)
                                    _logger.Operations(msg, e);

                                Statistics.RecordConsumeError(e.Message);

                                EnterFallbackMode();
                            }
                        }
                    }

                    bool batchStopped = false;
                    var processed = 0;

                    try
                    {
                        using (var tx = context.OpenWriteTransaction())
                        using (var scriptProcessingScope = stats.For(QueueSinkBatchPhases.ScriptProcessing))
                        {
                            var mainScript = new PatchRequest(Script.Script, PatchRequestType.QueueSink);
                            Database.Scripts.GetScriptRunner(mainScript, false, out var documentScript);

                            foreach (var message in messages)
                            {
                                try
                                {
                                    using (message)
                                    using (documentScript.Run(context, context, "execute", new object[] { message }))
                                    {
                                    }

                                    scriptProcessingScope.RecordProcessedMessage();
                                    processed++;
                                }
                                catch (JavaScriptParseException e)
                                {
                                    HandleScriptParseException(e);
                                    batchStopped = true;
                                }
                                catch (Exception e)
                                {
                                    var msg = "Failed to process consumed message by the script.";

                                    if (_logger.IsOperationsEnabled)
                                    {
                                        _logger.Operations(msg, e);
                                    }

                                    Statistics.RecordScriptExecutionError(e);
                                    scriptProcessingScope.RecordScriptError();
                                }
                            }

                            if (batchStopped == false)
                            {
                                tx.Commit();
                                _consumer.Commit();
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

                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(message, e);

                        Statistics.RecordConsumeError(e.Message);
                    }

                    statsAggregator.Complete();

                    Statistics.ConsumeSuccess(processed);
                    Database.QueueSinkLoader.OnBatchCompleted(Configuration.Name, Script.Name, Statistics);
                }
            }
            catch (Exception e)
            {
                var msg = $"Unexpected error in {Tag} process: '{Name}'";

                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations(msg, e);
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
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, _logger);
                NativeMemory.EnsureRegistered();
                Run();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to run Queue Sink {Name}", e);
            }
        }, null, ThreadNames.ForQueueSinkProcess(threadName, Tag, Name));

        if (_logger.IsOperationsEnabled)
            _logger.Operations($"Starting {Tag} process: '{Name}'.");

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

        using (context.OpenWriteTransaction())
        {
            using var messageDoc = context.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(testScript.Message)), "queue-sink-test-message");

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

        if (_logger.IsOperationsEnabled)
        {
            _logger.Operations(msg);
        }

        _cts.Cancel();

        var longRunningWork = _longRunningWork;
        _longRunningWork = null;

        if (longRunningWork != PoolOfThreads.LongRunningWork.Current) // prevent a deadlock
            longRunningWork.Join(int.MaxValue);
    }

    private void HandleScriptParseException(Exception e)
    {
        var message = $"[{Name}] Could not parse script. Stopping Queue Sink process.";

        if (_logger.IsOperationsEnabled)
            _logger.Operations(message, e);

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

        Statistics.RecordScriptExecutionError(e);

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

            if (_logger.IsInfoEnabled)
                _logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);

            return false;
        }

        if (_lowMemoryFlag.IsRaised() && batchSize >= MinBatchSize)
        {
            var reason = $"The batch was stopped after processing {batchSize:#,#;;0} items because of low memory";

            if (_logger.IsInfoEnabled)
                _logger.Info($"[{Name}] {reason}");

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
                    Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Database.ServerStore.Server.MetricCacher, _logger, out var memoryUsage) == false)
            {
                var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {totalAllocated}.";
                if (memoryUsage != null)
                {
                    reason += " Current memory usage: " +
                               $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                               $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"[{Name}] {reason}");

                stats.RecordPullCompleteReason(reason);

                ctx.DoNotReuse = true;

                return false;
            }
        }

        var maxBatchSize = Database.Configuration.QueueSink.MaxBatchSize;

        if (maxBatchSize != null && batchSize >= maxBatchSize)
        {
            var reason = $"Stopping the batch because maximum batch size limit was reached ({batchSize})";

            if (_logger.IsInfoEnabled)
                _logger.Info($"[{Name}] {reason}");

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

        var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {GetType().Name}: '{Name}'");

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
