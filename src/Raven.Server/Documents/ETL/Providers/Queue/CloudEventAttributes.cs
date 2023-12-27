using System;
using System.Collections.Generic;
using CloudNative.CloudEvents;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class CloudEventAttributes
{
    internal static string PartitionKeyLowercased = nameof(PartitionKey).ToLowerInvariant();

    public static HashSet<string> ValidAttributeNames = new()
    {
        // required
        nameof(Id), nameof(Type), nameof(Source),
        CloudEventsSpecVersion.V1_0.IdAttribute.Name, CloudEventsSpecVersion.V1_0.TypeAttribute.Name, CloudEventsSpecVersion.V1_0.SourceAttribute.Name,

        // optional
        nameof(PartitionKey), nameof(DataSchema), nameof(Subject), nameof(Time),
        PartitionKeyLowercased, CloudEventsSpecVersion.V1_0.DataSchemaAttribute.Name, CloudEventsSpecVersion.V1_0.SubjectAttribute.Name, CloudEventsSpecVersion.V1_0.TimeAttribute.Name,
    };

    public string Id { get; set; }

    public string Type { get; set; }

    public Uri Source { get; set; }

    public string PartitionKey { get; set; }

    public Uri DataSchema { get; set; }

    public string Subject { get; set; }

    public DateTimeOffset? Time { get; set; }
}
