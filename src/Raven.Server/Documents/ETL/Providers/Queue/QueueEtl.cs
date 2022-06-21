using System;
using System.Collections.Generic;
using System.Linq;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Amqp;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using Confluent.Kafka;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.Queue.Enumerators;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueEtl : EtlProcess<QueueItem, QueueWithMessages, QueueEtlConfiguration, QueueConnectionString, EtlStatsScope,
    EtlPerformanceOperation>
{
    public QueueEtl(Transformation transformation, QueueEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, QueueEtlTag)
    {
        Metrics = new EtlMetricsCountersManager();
    }

    private const string DefaultType = "ravendb.etl.put";

    public const string QueueEtlTag = "Queue ETL";
    private string TransactionalId => $"{Database.DbId}-{Name}";
    private readonly Dictionary<string, HashSet<string>> _existingRabbitMqQueuesAndExchanges = new();

    public override string EtlSubType => Configuration.BrokerType.ToString();

    private string DefaultSource =>
        $"{Database.Configuration.Core.PublicServerUrl?.UriValue ?? Database.ServerStore.Server.WebUrl}/{Database.Name}/{Name}";

    public override EtlType EtlType => EtlType.Queue;

    public override bool ShouldTrackCounters() => false;

    public override bool ShouldTrackTimeSeries() => false;

    protected override bool ShouldTrackAttachmentTombstones() => false;

    public override bool ShouldTrackDocumentTombstones() => false;

    protected override bool ShouldFilterOutHiLoDocument() => true;

    private IProducer<string, byte[]> _kafkaProducer;
    private IConnection _rabbitMqConnection;

    protected override IEnumerator<QueueItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs,
        string collection)
    {
        return new DocumentsToQueueItems(docs, collection);
    }

    protected override IEnumerator<QueueItem> ConvertTombstonesEnumerator(DocumentsOperationContext context,
        IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
    {
        throw new NotSupportedException("Tombstones aren't processed by Queue ETL currently");
    }

    protected override IEnumerator<QueueItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context,
        IEnumerator<Tombstone> tombstones, List<string> collections)
    {
        throw new NotSupportedException("Attachment tombstones aren't supported by Queue ETL");
    }

    protected override IEnumerator<QueueItem> ConvertCountersEnumerator(DocumentsOperationContext context,
        IEnumerator<CounterGroupDetail> counters, string collection)
    {
        throw new NotSupportedException("Counters aren't supported by Queue ETL");
    }

    protected override IEnumerator<QueueItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
    {
        throw new NotSupportedException("Time series aren't supported by Queue ETL");
    }

    protected override IEnumerator<QueueItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException("Time series aren't supported by Queue ETL");
    }

    protected override EtlTransformer<QueueItem, QueueWithMessages, EtlStatsScope, EtlPerformanceOperation> GetTransformer(
        DocumentsOperationContext context)
    {
        return new QueueDocumentTransformer(Transformation, Database, context, Configuration);
    }

    protected override int LoadInternal(IEnumerable<QueueWithMessages> items, DocumentsOperationContext context, EtlStatsScope scope)
    {
        int count = 0;

        var idsToDelete = new List<string>();

        using (var formatter = new BlittableJsonEventBinaryFormatter(context))
        {
            var publish = new QueueMessagesToPublish();

            foreach (QueueWithMessages queueItem in items)
            {
                var queueName = queueItem.Name;

                foreach (var message in queueItem.Messages)
                {
                    CloudEvent eventMessage = CreateEventMessage(message);

                    var messageToPublish = new QueueMessage
                    {
                        QueueName = queueName, 
                        CloudEvent = eventMessage,
                        Options = message.Options
                    };

                    publish.MessagesByLoadOrder.Add(messageToPublish);

                    if (publish.MessagesByQueue.TryGetValue(queueName, out var messagesPerQueue) == false)
                        publish.MessagesByQueue[queueName] = messagesPerQueue = new List<QueueMessage>();
                    
                    messagesPerQueue.Add(messageToPublish);

                    if (queueItem.DeleteProcessedDocuments)
                        idsToDelete.Add(message.DocumentId);
                }
            }

            switch (Configuration.Connection.BrokerType)
            {
                case QueueBrokerType.Kafka:
                    count = ProcessKafka(publish, formatter);
                    break;
                case QueueBrokerType.RabbitMq:
                    count = ProcessRabbitMq(publish, formatter);
                    break;
            }
        }

        if (idsToDelete.Count > 0)
        {
            var enqueue = Database.TxMerger.Enqueue(new DeleteDocumentsCommand(idsToDelete, Database));

            try
            {
                enqueue.GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                throw new QueueLoadException("Failed to delete processed documents", e);
            }
        }

        return count;
    }

    protected override EtlStatsScope CreateScope(EtlRunStats stats)
    {
        return new EtlStatsScope(stats);
    }

    private int ProcessKafka(QueueMessagesToPublish publish, BlittableJsonEventBinaryFormatter formatter)
    {
        int count = 0;

        if (publish.MessagesByLoadOrder.Count == 0)
            return count;

        if (_kafkaProducer == null)
        {
            var producer = QueueBrokerConnectionHelper.CreateKafkaProducer(Configuration.Connection.KafkaConnectionSettings, TransactionalId, Logger, Name,
                Database.ServerStore.Server.Certificate);

            try
            {
                producer.InitTransactions(TimeSpan.FromSeconds(60));
            }
            catch (Exception e)
            {
                string msg = $" ETL process: {Name}. Failed to initialize transactions for the producer instance. " +
                             $"If you are using a single node Kafka cluster then the following settings might be required:{Environment.NewLine}" +
                             $"- transaction.state.log.replication.factor: 1 {Environment.NewLine}" +
                             "- transaction.state.log.min.isr: 1";

                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                throw new QueueLoadException(msg, e);
            }

            _kafkaProducer = producer;
        }

        void ReportHandler(DeliveryReport<string, byte[]> report)
        {
            if (report.Error.IsError == false)
            {
                count++;
            }
            else
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to deliver message: {report.Error.Reason}");

                try
                {
                    _kafkaProducer.AbortTransaction();
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(
                            $"ETL process: {Name}. Aborting Kafka transaction failed after getting deliver report with error.", e);
                }
            }
        }

        _kafkaProducer.BeginTransaction();

        try
        {
            foreach (var @event in publish.MessagesByLoadOrder)
            {
                var kafkaMessage = @event.CloudEvent.ToKafkaMessage(ContentMode.Binary, formatter);

                _kafkaProducer.Produce(@event.QueueName, kafkaMessage, ReportHandler);
            }

            _kafkaProducer.CommitTransaction();
        }
        catch (Exception ex)
        {
            try
            {
                _kafkaProducer.AbortTransaction();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($" ETL process: {Name}. Aborting Kafka transaction failed.", e);
            }

            throw new QueueLoadException(ex.Message, ex);
        }

        return count;
    }

    private int ProcessRabbitMq(QueueMessagesToPublish publish, BlittableJsonEventBinaryFormatter formatter)
    {
        int count = 0;

        if (publish.MessagesByLoadOrder.Count == 0) 
            return count;

        if (_rabbitMqConnection == null)
        {
            _rabbitMqConnection = QueueBrokerConnectionHelper.CreateRabbitMqConnection(Configuration.Connection.RabbitMqConnectionSettings);
        }

        if (Configuration.SkipAutomaticQueueDeclaration == false)
            DeclareDefaultRabbitMqExchangesAndQueues(publish);

        // withing a single transaction we can publish messages to the same queue
        // so we handle the loaded messages per queue

        foreach (var item in publish.MessagesByQueue)
        {
            var queueName = item.Key;

            using var rabbitMqProducer = _rabbitMqConnection.CreateModel();
            
            rabbitMqProducer.TxSelect();

            var batch = rabbitMqProducer.CreateBasicPublishBatch();

            foreach (var message in item.Value)
            {
                var properties = rabbitMqProducer.CreateBasicProperties();
                properties.Headers = new Dictionary<string, object>();

                var rabbitMqMessage = message.CloudEvent.ToAmqpMessage(ContentMode.Binary, formatter);

                foreach (var appProperty in rabbitMqMessage.ApplicationProperties.Map.ToList())
                {
                    string key = appProperty.Key?.ToString();
                    string value = appProperty.Value?.ToString();
                    
                    if (key != null)
                    {
                        properties.Headers.Add(key, value);
                    }
                }
                
                batch.Add(message.Options?.Exchange ?? string.Empty, queueName, true, properties, new ReadOnlyMemory<byte>((byte[])rabbitMqMessage.Body));
                count++;
            }
            
            batch.Publish();
            try
            {
                rabbitMqProducer.TxCommit();
            }
            catch (Exception ex)
            {
                try
                {
                    rabbitMqProducer.TxRollback();
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($" ETL process: {Name}. Aborting RabbitMQ transaction failed.", e);
                }
                throw new QueueLoadException(ex.Message, ex);
            }
        }

        return count;
    }

    private void DeclareDefaultRabbitMqExchangesAndQueues(QueueMessagesToPublish publish)
    {
        using var rabbitMqModel = _rabbitMqConnection.CreateModel();
        
        foreach (var item in publish.MessagesByQueue)
        {
            var queueName = item.Key;
            var messages = item.Value;

            try
            {
                if (_existingRabbitMqQueuesAndExchanges.ContainsKey(queueName) == false)
                {
                    rabbitMqModel.QueueDeclare(queueName, true, false, false, null);

                    _existingRabbitMqQueuesAndExchanges.Add(queueName, new HashSet<string>());
                }

                foreach (var message in messages)
                {
                    var exchange = message.Options?.Exchange;

                    if (string.IsNullOrEmpty(exchange) == false && _existingRabbitMqQueuesAndExchanges[queueName].Contains(exchange) == false)
                    {
                        rabbitMqModel.ExchangeDeclare(exchange, ExchangeType.Direct, true, false, null);

                        rabbitMqModel.QueueBind(queueName, message.Options.Exchange, queueName);

                        _existingRabbitMqQueuesAndExchanges[queueName].Add(exchange);
                    }
                }
            }
            catch (OperationInterruptedException e)
            {
                if (e.ShutdownReason.ReplyCode == 406)
                {
                    // queue already exists
                    continue;
                }

                throw;
            }
        }
    }

    private CloudEvent CreateEventMessage(QueueItem item)
    {
        var eventMessage = new CloudEvent
        {
            Id = item.Options?.Id ?? item.ChangeVector,
            DataContentType = "application/json",
            Type = item.Options?.Type ?? DefaultType,
            Source = new Uri(item.Options?.Source ?? DefaultSource, UriKind.RelativeOrAbsolute),
            Data = item.TransformationResult
        };

        eventMessage.SetPartitionKey(item.Options?.PartitionKey ?? item.DocumentId);

        return eventMessage;
    }

    public QueueEtlTestScriptResult RunTest(IEnumerable<QueueWithMessages> records, DocumentsOperationContext context)
    {
        var simulatedWriter = new QueueWriterSimulator();
        var summaries = new List<QueueSummary>();

        foreach (var record in records)
        {
            var messages = simulatedWriter.SimulateExecuteMessages(record, context);
            summaries.Add(new QueueSummary { QueueName = record.Name, Messages = messages });
        }

        return new QueueEtlTestScriptResult
        {
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(), Summary = summaries
        };
    }

    protected override void OnProcessStopped()
    {
        _kafkaProducer?.Dispose();
        _kafkaProducer = null;
        _rabbitMqConnection?.Dispose();
        _rabbitMqConnection = null;
    }

    public override void Dispose()
    {
        base.Dispose();
        
        OnProcessStopped();
    }

    private class QueueMessagesToPublish
    {
        public List<QueueMessage> MessagesByLoadOrder { get; } = new();

        public Dictionary<string, List<QueueMessage>> MessagesByQueue { get; } = new();
    }

    private class QueueMessage
    {
        public string QueueName { get; set; }

        public QueueLoadOptions Options { get; set; }

        public CloudEvent CloudEvent { get; set; }
    }
}
