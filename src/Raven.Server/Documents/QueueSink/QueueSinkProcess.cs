using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Org.BouncyCastle.Utilities.IO.Pem;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink.Test;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using PemWriter = Org.BouncyCastle.OpenSsl.PemWriter;

namespace Raven.Server.Documents.QueueSink;

public class QueueSinkProcess : BackgroundWorkBase
{
    private QueueSinkProcess(QueueSinkConfiguration configuration, QueueSinkScript script,
        DocumentDatabase database, string resourceName, CancellationToken shutdown)
        : base(resourceName, shutdown)
    {
        Database = database;
        Configuration = configuration;
        Script = script;
        Name = $"{Configuration.Name}/{Script.Name}";
        Tag = "Kafka";
        Statistics = new QueueSinkProcessStatistics(Tag, Name, Database.NotificationCenter);
    }

    public static QueueSinkProcess CreateInstance(QueueSinkScript script, QueueSinkConfiguration configuration,
        DocumentDatabase database)
    {
        return new QueueSinkProcess(configuration, script, database, null, database.DatabaseShutdown);
    }

    private IConsumer<string, byte[]> _consumer;

    private TestMode _testMode;

    private string GroupId => $"{Database.DatabaseGroupId}/{Name}";

    public DocumentDatabase Database { get; }

    public QueueSinkProcessStatistics Statistics { get; }

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
        ProcessFallback();
        bool batchStopped = false;

        if (_consumer == null)
        {
            try
            {
                _consumer = CreateKafkaConsumer();
            }
            catch (Exception e)
            {
                string msg = $"Failed to create kafka consumer for {Name}.";

                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                EnterFallbackMode();
                return;
            }
        }

        var messageBatch = new List<ConsumeResult<string, byte[]>>();

        while (messageBatch.Count < Database.Configuration.QueueSink.MaxNumberOfConsumedMessagesInBatch)
        {
            try
            {
                var message = messageBatch.Count == 0
                    ? _consumer.Consume(CancellationToken)
                    : _consumer.Consume(TimeSpan.Zero);
                if (message?.Message is null) break;
                messageBatch.Add(message);
            }
            catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                string msg = $"Failed to consume message.";
                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                EnterFallbackMode();
                Statistics.RecordConsumeError(e.Message, 0);
                return;
            }
        }

        if (messageBatch.Count == 0) return;

        try
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(
                       out DocumentsOperationContext context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    var mainScript = new PatchRequest(Configuration.Scripts.First().Script,
                        PatchRequestType.QueueSink);
                    Database.Scripts.GetScriptRunner(mainScript, false, out var documentScript);

                    foreach (var message in messageBatch)
                    {
                        try
                        {
                            using var o = await context.ReadForMemoryAsync(new MemoryStream(message.Message.Value),
                                "queue-message", CancellationToken);
                            using (documentScript.Run(context, context, "execute", new object[] { o })) { }
                        }
                        catch (JavaScriptParseException e)
                        {
                            HandleScriptParseException(e);
                            batchStopped = true;
                        }
                        catch (Exception e)
                        {
                            var msg = "Failed to process consumed message.";
                            if (Logger.IsOperationsEnabled)
                            {
                                Logger.Operations(msg, e);
                            }

                            Statistics.RecordScriptExecutionError(e);
                        }
                    }

                    if (batchStopped == false)
                    {
                        tx.Commit();
                        _consumer.Commit();
                    }
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

    public static IDisposable TestScript(TestQueueSinkScript testScript, DocumentsOperationContext context, DocumentDatabase database,
        out TestQueueSinkScriptResult result)
    {
        result = new TestQueueSinkScriptResult();
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

        List<string> debugOutput;
        
        //using (EnterTestMode(out debugOutput))
        {
            //result.DebugOutput = debugOutput;

            try
            {
                var mainScript = new PatchRequest(testScript.Script, PatchRequestType.QueueSink);
                database.Scripts.GetScriptRunner(mainScript, false, out var documentScript);
                
                /*using var o = context.ReadForMemoryAsync(new MemoryStream(message),
                    "queue-message", CancellationToken);
                using (documentScript.Run(context, context, "execute", new object[] { o })) { }*/
            }
            catch (JavaScriptParseException e)
            {
                
            }
        }

        return null;
    }

    private IDisposable EnterTestMode(out List<string> debugOutput)
    {
        _testMode = new TestMode();
        var disableAlerts = Statistics.PreventFromAddingAlertsToNotificationCenter();

        debugOutput = _testMode.DebugOutput;

        return new DisposableAction(() =>
        {
            _testMode = null;
            disableAlerts.Dispose();
        });
    }

    private IConsumer<string, byte[]> CreateKafkaConsumer()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = Configuration.Connection.KafkaConnectionSettings.BootstrapServers,
            GroupId = GroupId,
            IsolationLevel = IsolationLevel.ReadCommitted,
            // we are disabling auto commit option and we are manually commit only messages that are processed successfully
            EnableAutoCommit = false,
            // we are using Earliest option because we want to be able to see messages which are present before consumer is connected
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        var settings = Configuration.Connection.KafkaConnectionSettings;
        var certificateHolder = Database.ServerStore.Server.Certificate;
        
        if (settings.UseRavenCertificate && certificateHolder?.Certificate != null)
        {
            consumerConfig.SslCertificatePem = ExportAsPem(new PemObject("CERTIFICATE", certificateHolder.Certificate.RawData));
            consumerConfig.SslKeyPem = ExportAsPem(certificateHolder.PrivateKey.Key);
            consumerConfig.SecurityProtocol = SecurityProtocol.Ssl;
        }

        if (settings.ConnectionOptions != null)
        {
            foreach (KeyValuePair<string, string> option in settings.ConnectionOptions)
            {
                consumerConfig.Set(option.Key, option.Value);
            }
        }

        var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(Script.Queues);
        return consumer;
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
    
    private static string ExportAsPem(object @object)
    {
        using (var sw = new StringWriter())
        {
            var pemWriter = new PemWriter(sw);
            
            pemWriter.WriteObject(@object);

            return sw.ToString();
        }
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
            // double the fallback time (but don't cross Etl.MaxFallbackTime)
            var secondsSinceLastError =
                (Database.Time.GetUtcNow() - Statistics.LastConsumeErrorTime.Value).TotalSeconds;

            FallbackTime = TimeSpan.FromSeconds(Math.Min(
                Database.Configuration.QueueSink
                    .MaxFallbackTime.AsTimeSpan.TotalSeconds,
                Math.Max(5, secondsSinceLastError * 2)));
        }
    }

    private void ProcessFallback()
    {
        if (FallbackTime != null)
        {
            Thread.Sleep(FallbackTime.Value);
            FallbackTime = null;
        }
    }

    private class TestMode
    {
        public readonly List<string> DebugOutput = new();
    }
}
