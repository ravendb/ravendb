using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class CloudEventAttributes
{
    public static HashSet<string> ValidAttributeNames = new()
    {
        nameof(Id), nameof(Type), nameof(Source), nameof(PartitionKey)
    };

    public string Id { get; set; }

    public string Type { get; set; }

    public string Source { get; set; }

    public string PartitionKey { get; set; }
}
