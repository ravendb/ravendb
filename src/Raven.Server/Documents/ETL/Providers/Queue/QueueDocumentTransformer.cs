using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public abstract class QueueDocumentTransformer<T, TSelf, TJsType> : EtlTransformer<QueueItem, QueueWithItems<T>, EtlStatsScope, EtlPerformanceOperation, TJsType>
    where T : QueueItem
    where TSelf : QueueItem
    where TJsType : struct, IJsHandle<TJsType>
{
    private readonly QueueEtlConfiguration _config;
    private readonly Dictionary<string, QueueWithItems<TSelf>> _queues;

    protected QueueDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config)
        : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.QueueEtl))
    {
        _config = config;

        var destinationQueues = transformation.GetCollectionsFromScript();

        LoadToDestinations = destinationQueues;

        _queues = new Dictionary<string, QueueWithItems<TSelf>>(destinationQueues.Length, StringComparer.OrdinalIgnoreCase);
    }

    protected override void AddLoadedAttachment(TJsType reference, string name, Attachment attachment)
    {
        throw new NotSupportedException("Attachments aren't supported by Queue ETL");
    }

    protected override void AddLoadedCounter(TJsType reference, string name, long value)
    {
        throw new NotSupportedException("Counters aren't supported by Queue ETL");
    }

    protected override void AddLoadedTimeSeries(TJsType reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new NotSupportedException("Time series aren't supported by Queue ETL");
    }

    protected override string[] LoadToDestinations { get; }

    public override IEnumerable<QueueWithItems<T>> GetTransformedResults()
    {
        foreach (QueueWithItems<TSelf> item in _queues.Values)
        {
            yield return item as QueueWithItems<T>; // T and TSelf are the same types so it will never return null
        }
    }

    public override void Transform(QueueItem item, EtlStatsScope stats, EtlProcessState state)
    {
        if (item.IsDelete == false)
        {
            Current = item;
            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
        }
        else
        {
            throw new NotSupportedException("Processing of document tombstones is not currently supported");
        }
    }

    protected QueueWithItems<TSelf> GetOrAdd(string queueName)
    {
        if (_queues.TryGetValue(queueName, out QueueWithItems<TSelf> queue) == false)
        {
            var etlQueue = _config.Queues?.Find(x => x.Name.Equals(queueName, StringComparison.OrdinalIgnoreCase));

            _queues[queueName] = queue = new QueueWithItems<TSelf>(etlQueue ?? new EtlQueue { Name = queueName });
        }

        return queue;
    }

    protected CloudEventAttributes GetCloudEventAttributes(TJsType attributes)
    {
        var cloudEventAttributes = new CloudEventAttributes();

        foreach (var x in attributes.GetOwnProperties().Select(x => x.Key))
        {
            if (CloudEventAttributes.ValidAttributeNames.Contains(x) == false)
                throw new InvalidOperationException(
                    $"Unknown attribute passed to loadTo(..., {{ {x}: ... }}). '{x}' is not a valid attribute name (note: field names are case sensitive)");

            if (TryGetOptionValue(nameof(CloudEventAttributes.Id), out var messageId))
                cloudEventAttributes.Id = messageId;

            if (TryGetOptionValue(nameof(CloudEventAttributes.Type), out var type))
                cloudEventAttributes.Type = type;

            if (TryGetOptionValue(nameof(CloudEventAttributes.Source), out var source))
                cloudEventAttributes.Source = source;

            if (TryGetOptionValue(nameof(CloudEventAttributes.PartitionKey), out var partitionKey))
                cloudEventAttributes.PartitionKey = partitionKey;
        }

        return cloudEventAttributes;

        bool TryGetOptionValue(string optionName, out string value)
        {
            var optionValue = attributes.GetOwnProperty(optionName);

            if (optionValue.IsNull == false && optionValue.IsUndefined == false)
            {
                value = optionValue.AsString;
                return true;
            }

            value = null;
            return false;
        }
    }
}
