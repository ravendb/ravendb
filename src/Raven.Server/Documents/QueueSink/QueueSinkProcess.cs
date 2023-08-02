
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Serialization;
using Raven.Server.Background;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink.Stats;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Documents.QueueSink.Test;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.QueueSink;

public abstract class QueueSinkProcess : BackgroundWorkBase
{
    private int _statsId;
    private QueueSinkStatsAggregator _lastStats;

    private readonly ConcurrentQueue<QueueSinkStatsAggregator> _lastQueueSinkStats =
        new ConcurrentQueue<QueueSinkStatsAggregator>();

    private IQueueSinkConsumer _consumer;

    protected QueueSinkProcess(QueueSinkConfiguration configuration, QueueSinkScript script,
        DocumentDatabase database, string tag, CancellationToken shutdown)
        : base($"[{tag}] {configuration.Name}/{script.Name}", shutdown)
    {
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
                return new KafkaQueueSink(configuration, script, database, "Kafka Sink", database.DatabaseShutdown);
            case QueueBrokerType.RabbitMq:
                return new RabbitMqQueueSink(configuration, script, database, "RabbitMQ Sink", database.DatabaseShutdown);
            default:
                throw new NotSupportedException($"Unknown broker type: {configuration.BrokerType}");
        }
    }

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

    protected override async Task DoWork()
    {
        if (_consumer == null)
        {
            try
            {
                _consumer = CreateConsumer();
            }
            catch (Exception e)
            {
                string msg = $"Failed to create consumer for {Name}.";

                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                EnterFallbackMode();

                return;
            }
        }

        var statsAggregator = new QueueSinkStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);
        _lastStats = statsAggregator;

        AddPerformanceStats(statsAggregator);
        
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var stats = statsAggregator.CreateScope())
        {
            var messageBatch = new List<BlittableJsonReaderObject>();
            
            using (var consumeScope = stats.For(QueueSinkBatchPhases.Consume, start: false))
            {
                while (messageBatch.Count < Database.Configuration.QueueSink.MaxNumberOfConsumedMessagesInBatch && CancellationToken.IsCancellationRequested == false)
                {
                    try
                    {
                        var message = messageBatch.Count == 0
                            ? _consumer.Consume(CancellationToken)
                            : _consumer.Consume(TimeSpan.Zero);

                        if (message is null)
                            break;

                        if (consumeScope.IsRunning == false)
                            consumeScope.Start();

                        var jsonMessage = await context.ReadForMemoryAsync(new MemoryStream(message), "queue-message", CancellationToken);

                        messageBatch.Add(jsonMessage);

                        consumeScope.RecordConsumedMessage();

                        // TODO arek - can continue batch
                    }
                    catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception e)
                    {
                        string msg = "Failed to consume message.";

                        if (Logger.IsOperationsEnabled)
                        {
                            Logger.Operations(msg, e);
                        }

                        EnterFallbackMode();
                        Statistics.RecordConsumeError(e.Message, 0);
                    }
                }
            }

            if (messageBatch.Count == 0)
                return;

            bool batchStopped = false;

            try
            {
                
                    using (var tx = context.OpenWriteTransaction())
                    using (var scriptProcessingScope = stats.For(QueueSinkBatchPhases.ScriptProcessing))
                    {
                        var mainScript = new PatchRequest(Script.Script, PatchRequestType.QueueSink);
                        Database.Scripts.GetScriptRunner(mainScript, false, out var documentScript);

                        foreach (var message in messageBatch)
                        {
                            try
                            {
                                using (message)
                                using (documentScript.Run(context, context, "execute", new object[] {message}))
                                {
                                }

                                scriptProcessingScope.RecordProcessedMessage();
                            }
                            catch (JavaScriptParseException e)
                            {
                                HandleScriptParseException(e);
                                batchStopped = true;
                            }
                            catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                var msg = "Failed to process consumed message by the script.";

                                if (Logger.IsOperationsEnabled)
                                {
                                    Logger.Operations(msg, e);
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
            catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                var message = $"{Tag} Exception in queue sink process '{Name}'";

                if (Logger.IsOperationsEnabled)
                    Logger.Operations(message, e);

                Statistics.RecordConsumeError(e.Message);
            }

            Statistics.ConsumeSuccess(messageBatch.Count);
            Database.QueueSinkLoader.OnBatchCompleted(Configuration.Name, Script.Name, Statistics);
        }
    }

    private void AddPerformanceStats(QueueSinkStatsAggregator stats)
    {
        _lastQueueSinkStats.Enqueue(stats);

        while (_lastQueueSinkStats.Count > 25)
            _lastQueueSinkStats.TryDequeue(out _);
    }

    protected abstract IQueueSinkConsumer CreateConsumer();

    private class TestQueueMessageCommand : PatchDocumentCommand
    {
        private readonly BlittableJsonReaderObject _message;

        public TestQueueMessageCommand(JsonOperationContext context, PatchRequest patch, BlittableJsonReaderObject message) : base(context, Guid.NewGuid().ToString(),
            null, false, (patch, null), (null, null), null, '/', isTest: true, debugMode: true, collectResultsNeeded: true, returnDocument: true)
        {
            _message = message;
        }

        protected override Document GetCurrentDocument(DocumentsOperationContext context, string id)
        {
            return new Document
            {
                Data = _message
            };
        }
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
        string msg = $"Stopping {Tag} process: '{Name}'. Reason: {reason}";

        if (Logger.IsOperationsEnabled)
        {
            Logger.Operations(msg);
        }

        base.Stop();
    }

    private void HandleScriptParseException(Exception e)
    {
        var message = $"[{Name}] Could not parse script. Stopping Queue Sink process.";

        if (Logger.IsOperationsEnabled)
            Logger.Operations(message, e);

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

        Thread.Sleep(FallbackTime.Value);
        FallbackTime = null;
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

    public override void Dispose()
    {
        base.Dispose();

        _consumer?.Dispose();
    }
}
