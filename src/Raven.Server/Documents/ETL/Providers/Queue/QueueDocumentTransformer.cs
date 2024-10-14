using System;
using System.Collections.Generic;
using CloudNative.CloudEvents;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public abstract class QueueDocumentTransformer<T, TSelf> : EtlTransformer<QueueItem, QueueWithItems<T>, EtlStatsScope, EtlPerformanceOperation>
where T : QueueItem
where TSelf : QueueItem
{
    private readonly QueueEtlConfiguration _config;
    private readonly Dictionary<string, QueueWithItems<TSelf>> _queues;

    protected QueueDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config)
        : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.QueueEtl), null)
    {
        _config = config;

        var destinationQueues = transformation.GetCollectionsFromScript();

        LoadToDestinations = destinationQueues;

        _queues = new Dictionary<string, QueueWithItems<TSelf>>(destinationQueues.Length, StringComparer.OrdinalIgnoreCase);
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        throw new NotSupportedException("Attachments aren't supported by Queue ETL");
    }

    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new NotSupportedException("Counters aren't supported by Queue ETL");
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
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

    protected CloudEventAttributes GetCloudEventAttributes(ObjectInstance attributes)
    {
        var cloudEventAttributes = new CloudEventAttributes();

        foreach (var property in attributes.GetOwnProperties())
        {
            var attributeName = property.Key.ToString();
            var attributeValue = property.Value;

            if (CloudEventAttributes.ValidAttributeNames.Contains(attributeName) == false)
                throw new InvalidOperationException(@$"Unknown attribute passed to loadTo(..., {{ {attributeName}: ... }}). '{attributeName}' is not a valid attribute name");

            if (attributeName is nameof(CloudEventAttributes.Time) || attributeName == CloudEventsSpecVersion.V1_0.TimeAttribute.Name)
            {
                if (TryGetDateAttributeValue(attributeValue, out var time))
                {
                    CloudEventsSpecVersion.V1_0.TimeAttribute.Validate(time);
                    cloudEventAttributes.Time = time;
                }
            }
            else
            {
                if (TryGetStringAttributeValue(attributeValue, out string value) == false)
                    continue;

                if (attributeName is nameof(CloudEventAttributes.Id) || attributeName == CloudEventsSpecVersion.V1_0.IdAttribute.Name)
                {
                    CloudEventsSpecVersion.V1_0.IdAttribute.Validate(value);
                    cloudEventAttributes.Id = value;
                }
                else if (attributeName is nameof(CloudEventAttributes.Type) || attributeName == CloudEventsSpecVersion.V1_0.TypeAttribute.Name)
                {
                    CloudEventsSpecVersion.V1_0.TypeAttribute.Validate(value);
                    cloudEventAttributes.Type = value;
                }
                else if (attributeName is nameof(CloudEventAttributes.PartitionKey) || attributeName == CloudEventAttributes.PartitionKeyLowercased)
                {
                    cloudEventAttributes.PartitionKey = value;
                }
                else if (attributeName is nameof(CloudEventAttributes.Source) || attributeName == CloudEventsSpecVersion.V1_0.SourceAttribute.Name)
                {
                    var sourceUri = new Uri(value, UriKind.RelativeOrAbsolute);
                    CloudEventsSpecVersion.V1_0.SourceAttribute.Validate(sourceUri);
                    cloudEventAttributes.Source = sourceUri;
                }
                else if (attributeName is nameof(CloudEventAttributes.DataSchema) || attributeName == CloudEventsSpecVersion.V1_0.DataSchemaAttribute.Name)
                {
                    var dataSchemaUri = new Uri(value, UriKind.RelativeOrAbsolute);
                    CloudEventsSpecVersion.V1_0.DataSchemaAttribute.Validate(dataSchemaUri);
                    cloudEventAttributes.DataSchema = dataSchemaUri;
                }
                else if (attributeName is nameof(CloudEventAttributes.Subject) || attributeName == CloudEventsSpecVersion.V1_0.SubjectAttribute.Name)
                {
                    CloudEventsSpecVersion.V1_0.SubjectAttribute.Validate(value);
                    cloudEventAttributes.Subject = value;
                }
            }
        }

        return cloudEventAttributes;

        bool TryGetStringAttributeValue(PropertyDescriptor attributeValue, out string value)
        {
            var optionValue = attributeValue.Value;

            if (optionValue != null && optionValue.IsNull() == false && optionValue.IsUndefined() == false)
            {
                if (optionValue.IsString())
                {
                    value = optionValue.AsString();
                    return true;
                }
            }

            value = null;
            return false;
        }

        bool TryGetDateAttributeValue(PropertyDescriptor attributeValue, out DateTimeOffset value)
        {
            var optionValue = attributeValue.Value;

            if (optionValue != null && optionValue.IsNull() == false && optionValue.IsUndefined() == false)
            {
                if (optionValue.IsString())
                {
                    if (DateTimeOffset.TryParse(optionValue.AsString(), out var dateTimeOffset) == false)
                        throw new ArgumentException($"Invalid format of date attribute: {optionValue.AsString()}");

                    value = dateTimeOffset;
                    return true;

                }
                if (optionValue.IsDate())
                {
                    var dateInstance = optionValue.AsDate();
                    value = dateInstance.ToDateTime();

                    return true;
                }
            }

            value = default;
            return false;
        }
    }
}
