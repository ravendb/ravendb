using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amqp;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Amqp;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using Confluent.Kafka;
using EasyNetQ;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.Queue.Enumerators;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueEtl : EtlProcess<QueueItem, QueueWithEvents, QueueEtlConfiguration, QueueConnectionString, EtlStatsScope,
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

    private string DefaultSource =>
        $"{Database.Configuration.Core.PublicServerUrl?.UriValue ?? Database.ServerStore.Server.WebUrl}/{Database.Name}/{Name}";
    public override EtlType EtlType => EtlType.Queue;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;
    protected override bool ShouldTrackAttachmentTombstones() => false;
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
        //todo djordje: ignore tombstones
        return new TombstonesToQueueItems(tombstones, collection);
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

    protected override EtlTransformer<QueueItem, QueueWithEvents, EtlStatsScope, EtlPerformanceOperation> GetTransformer(
        DocumentsOperationContext context)
    {
        return new QueueDocumentTransformer(Transformation, Database, context, Configuration);
    }

    protected override int LoadInternal(IEnumerable<QueueWithEvents> items, DocumentsOperationContext context, EtlStatsScope scope)
    {
        int count = 0;

        switch (Configuration.Connection.BrokerType)
        {
            case QueueBroker.Kafka:
                count = ProcessKafka(items, Configuration, context);
                break;
            case QueueBroker.RabbitMq:
                ProcessRabbitMq(items, context);
                break;
        }

        return count;
    }

    protected override EtlStatsScope CreateScope(EtlRunStats stats)
    {
        return new EtlStatsScope(stats);
    }

    private int ProcessKafka(IEnumerable<QueueWithEvents> items, QueueEtlConfiguration queueEtlConfiguration,
        DocumentsOperationContext context)
    {
        int count = 0;

        using (var formatter = new BlittableEventBinaryFormatter(context))
        {
            var events = new List<KafkaMessageEvent>();

            foreach (QueueWithEvents queueItem in items)
            {
                var topicName = queueItem.Name;

                foreach (var insertItem in queueItem.Inserts)
                {
                    CloudEvent eventMessage = CreateEventMessage(insertItem);

                    events.Add(new KafkaMessageEvent
                    {
                        Topic = topicName, Message = eventMessage.ToKafkaMessage(ContentMode.Binary, formatter)
                    });
                }
            }

            if (events.Count == 0) return count;

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
                foreach (var @event in events)
                {
                    _kafkaProducer.Produce(@event.Topic, @event.Message, ReportHandler);
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
        }

        return count;
    }

    private int ProcessRabbitMq(IEnumerable<QueueWithEvents> items, DocumentsOperationContext context)
    {
        int count = 0;

        using (var formatter = new BlittableEventBinaryFormatter(context))
        {
            var events = new List<RabbitMqMessageEvent>();

            foreach (QueueWithEvents queueItem in items)
            {
                var topicName = queueItem.Name;

                foreach (var insertItem in queueItem.Inserts)
                {
                    CloudEvent eventMessage = CreateEventMessage(insertItem);

                    events.Add(new RabbitMqMessageEvent
                    {
                        Topic = topicName, Message = eventMessage.ToAmqpMessage(ContentMode.Binary, formatter)
                    });
                }
            }

            if (events.Count == 0) return count;

            if (_rabbitMqProducer == null)
            {
                _rabbitMqProducer = QueueHelper.CreateRabbitMqClient(Configuration.Connection, TransactionalId,
                    Database.ServerStore.Server.Certificate.Certificate);
            }

            foreach (var @event in events)
            {
                //loadToOrders
                //_rabbitMqProducer.QueueDeclare(@event.Topic, durable: false, exclusive: false, autoDelete: false, arguments: null);
                _rabbitMqProducer.BasicPublish("Test-topic", "", null, @event.Message.Encode().Buffer);
            }
        }

        return count;
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

    public QueueEtlTestScriptResult RunTest(IEnumerable<QueueWithEvents> records, DocumentsOperationContext context)
    {
        var simulatedWriter = new QueueWriterSimulator();
        var summaries = new List<TopicSummary>();

        foreach (var record in records)
        {
            var commands = simulatedWriter.SimulateExecuteCommandText(record, context);

            summaries.Add(new TopicSummary { TopicName = record.Name, Commands = commands.ToArray() });
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
    }
}
