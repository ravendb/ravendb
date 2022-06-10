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
    public const string DefaultExchangeType = "direct";
    private string TransactionalId => $"{Database.DbId}-{Name}";
    private readonly HashSet<string> _existingQueues = new();
    private readonly HashSet<Tuple<string, string>> _existingExchanges = new();

    private string DefaultSource =>
        $"{Database.Configuration.Core.PublicServerUrl?.UriValue ?? Database.ServerStore.Server.WebUrl}/{Database.Name}/{Name}";

    public override EtlType EtlType => EtlType.Queue;

    public override bool ShouldTrackCounters() => false;

    public override bool ShouldTrackTimeSeries() => false;

    protected override bool ShouldTrackAttachmentTombstones() => false;

    public override bool ShouldTrackDocumentTombstones() => false;

    protected override bool ShouldFilterOutHiLoDocument() => true;

    private IProducer<string, byte[]> _kafkaProducer;
    private IModel _rabbitMqProducer;

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
            var messages = new List<QueueMessage>();

            foreach (QueueWithMessages queueItem in items)
            {
                var queueName = queueItem.Name;

                foreach (var message in queueItem.Messages)
                {
                    CloudEvent eventMessage = CreateEventMessage(message);

                    messages.Add(new QueueMessage { QueueName = queueName, Message = eventMessage });

                    if (queueItem.DeleteProcessedDocuments)
                        idsToDelete.Add(message.DocumentId);
                }
            }

            switch (Configuration.Connection.BrokerType)
            {
                case QueueBroker.Kafka:
                    count = ProcessKafka(messages, formatter);
                    break;
                case QueueBroker.RabbitMq:
                    count = ProcessRabbitMq(items, formatter);
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

    private int ProcessKafka(List<QueueMessage> messages, BlittableJsonEventBinaryFormatter formatter)
    {
        int count = 0;

        if (messages.Count == 0)
            return count;

        if (_kafkaProducer == null)
        {
            _kafkaProducer = QueueHelper.CreateKafkaClient(Configuration.Connection, TransactionalId, Logger,
                Database.ServerStore.Server.Certificate.Certificate);
            _kafkaProducer.InitTransactions(TimeSpan.FromSeconds(60));
        }

        void ReportHandler(DeliveryReport<string?, byte[]> report)
        {
            if (report.Error.IsError == false)
            {
                count++;
            }
            else
            {
                Logger.Info($"Failed to deliver message: {report.Error.Reason}");
                _kafkaProducer.AbortTransaction();
            }
        }

        _kafkaProducer.BeginTransaction();

        try
        {
            foreach (var @event in messages)
            {
                var kafkaMessage = @event.Message.ToKafkaMessage(ContentMode.Binary, formatter);

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
                Logger.Info("Aborting kafka transaction failed.", e);
            }

            throw new QueueLoadException(ex.Message, ex);
        }

        return count;
    }

    private int ProcessRabbitMq(IEnumerable<QueueWithMessages> items, BlittableJsonEventBinaryFormatter formatter)
    {
        int count = 0;

        var messages = new List<RabbitMqMessageEvent>();

        foreach (QueueWithMessages queueItem in items)
        {
            var newMessage = new RabbitMqMessageEvent() { Queue = queueItem.Name };

            foreach (var message in queueItem.Messages)
            {
                CloudEvent eventMessage = CreateEventMessage(message);
                newMessage.Messages.Add(new RabbitMqMessage()
                {
                    Exchange = message.Options?.Exchange ?? "",
                    ExchangeType = message.Options?.ExchangeType ?? DefaultExchangeType,
                    Message = eventMessage.ToAmqpMessage(ContentMode.Binary, formatter)
                });
            }

            messages.Add(newMessage);
        }

        if (messages.Count == 0) return count;

        if (_rabbitMqProducer == null)
        {
            _rabbitMqProducer = QueueHelper.CreateRabbitMqClient(Configuration.Connection, TransactionalId);
        }

        // todo: check application properties, _rabbitMqProducer.CreateBasicProperties();

        SetUpExchangesAndQueues(messages);

        var batch = _rabbitMqProducer.CreateBasicPublishBatch();
        foreach (var message in messages)
        {
            foreach (var messageContent in message.Messages)
            {
                batch.Add(messageContent.Exchange, message.Queue, true, null,
                    new ReadOnlyMemory<byte>((byte[])messageContent.Message.Body));
                count++;
            }
        }
        batch.Publish();
        return count;
    }

    private void SetUpExchangesAndQueues(List<RabbitMqMessageEvent> messages)
    {
        foreach (var message in messages)
        {
            try
            {
                if (_existingQueues.Contains(message.Queue) == false)
                {
                    _rabbitMqProducer.QueueDeclare(message.Queue, true, false, false, null);
                    _existingQueues.Add(message.Queue);
                }

                foreach (var messageContent in message.Messages)
                {
                    if (string.IsNullOrWhiteSpace(messageContent.Exchange) == false)
                    {
                        if (_existingExchanges.Contains(Tuple.Create(messageContent.Exchange, message.Queue)) == false)
                        {
                            _rabbitMqProducer.ExchangeDeclare(messageContent.Exchange, messageContent.ExchangeType, true, false, null);
                            _rabbitMqProducer.QueueBind(message.Queue, messageContent.Exchange, message.Queue);
                            _existingExchanges.Add(Tuple.Create(messageContent.Exchange, message.Queue));
                        }
                    }
                }
            }
            catch (OperationInterruptedException e)
            {
                if (e.ShutdownReason.ReplyCode != 406)
                {
                    throw;
                }
            }
            catch (Exception)
            {
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

        eventMessage.SetPartitionKey(item.Options?.PartitionKey ?? item.ChangeVector);

        return eventMessage;
    }

    public QueueEtlTestScriptResult RunTest(IEnumerable<QueueWithMessages> records, DocumentsOperationContext context)
    {
        var simulatedWriter = new QueueWriterSimulator();
        var summaries = new List<TopicSummary>();

        foreach (var record in records)
        {
            var messages = simulatedWriter.SimulateExecuteCommandText(record, context);
            // todo: message should have body (content) and options

            summaries.Add(new TopicSummary { TopicName = record.Name, Commands = messages.ToArray() });
        }

        return new QueueEtlTestScriptResult
        {
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(), Summary = summaries
        };
    }

    public override void Dispose()
    {
        base.Dispose();
        _kafkaProducer?.Dispose();
        _rabbitMqProducer?.Dispose();
    }

    private class QueueMessage
    {
        public string QueueName { get; set; }

        public CloudEvent Message { get; set; }
    }
}
