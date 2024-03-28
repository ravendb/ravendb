using System;
using System.Collections.Generic;
using System.Linq;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.Queue.AzureQueueStorage;
using Raven.Server.Documents.ETL.Providers.Queue.Enumerators;
using Raven.Server.Documents.ETL.Providers.Queue.Kafka;
using Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public abstract class QueueEtl<T> : EtlProcess<QueueItem, QueueWithItems<T>, QueueEtlConfiguration, QueueConnectionString, EtlStatsScope,
    EtlPerformanceOperation> where T : QueueItem
{

    public const string QueueEtlTag = "Queue ETL";
    private const string DefaultCloudEventType = "ravendb.etl.put";

    protected QueueEtl(Transformation transformation, QueueEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, QueueEtlTag)
    {
        Metrics = new EtlMetricsCountersManager();

        DefaultCloudEventSource = new Uri($"{Database.Configuration.Core.PublicServerUrl?.UriValue ?? Database.ServerStore.Server.WebUrl}/{Database.Name}/{Name}",
            UriKind.RelativeOrAbsolute);
    }

    public static EtlProcess CreateInstance(Transformation transformation, QueueEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
    {
        switch (configuration.BrokerType)
        {
            case QueueBrokerType.Kafka:
                return new KafkaEtl(transformation, configuration, database, serverStore);
            case QueueBrokerType.RabbitMq:
                return new RabbitMqEtl(transformation, configuration, database, serverStore);
            case QueueBrokerType.AzureQueueStorage:
                return new AzureQueueStorageEtl(transformation, configuration, database, serverStore);
            default:
                throw new NotSupportedException($"Unknown broker type: {configuration.BrokerType}");
        }
    }

    private Uri  DefaultCloudEventSource { get; }

    public override string EtlSubType => Configuration.BrokerType.ToString();

    public override EtlType EtlType => EtlType.Queue;

    public override bool ShouldTrackCounters() => false;

    public override bool ShouldTrackTimeSeries() => false;

    protected override bool ShouldTrackAttachmentTombstones() => false;

    public override bool ShouldTrackDocumentTombstones() => false;

    protected override bool ShouldFilterOutHiLoDocument() => true;

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

    protected override int LoadInternal(IEnumerable<QueueWithItems<T>> items, DocumentsOperationContext context, EtlStatsScope scope)
    {
        using (var formatter = new BlittableJsonEventBinaryFormatter(context))
        {
            var count = PublishMessages(items.ToList(), formatter, out var idsToDelete);

            if (idsToDelete is {Count: > 0})
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
    }

    protected abstract int PublishMessages(List<QueueWithItems<T>> items, BlittableJsonEventBinaryFormatter formatter, out List<string> idsToDelete);

    protected override EtlStatsScope CreateScope(EtlRunStats stats)
    {
        return new EtlStatsScope(stats);
    }

    protected CloudEvent CreateCloudEvent(QueueItem item)
    {
        var eventMessage = new CloudEvent
        {
            Data = item.TransformationResult,

            // required attributes
            Id = item.Attributes?.Id ?? item.ChangeVector,
            DataContentType = "application/json",
            Type = item.Attributes?.Type ?? DefaultCloudEventType,
            Source = item.Attributes?.Source ?? DefaultCloudEventSource,

            // optional attributes
            DataSchema = item.Attributes?.DataSchema,
            Subject = item.Attributes?.Subject,
            Time = item.Attributes?.Time
        };

        eventMessage.SetPartitionKey(item.Attributes?.PartitionKey ?? item.DocumentId);

        return eventMessage;
    }

    public QueueEtlTestScriptResult RunTest(IEnumerable<QueueWithItems<T>> records, DocumentsOperationContext context)
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
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
            Summary = summaries
        };
    }

    public override void Dispose()
    {
        base.Dispose();

        OnProcessStopped();
    }
}
