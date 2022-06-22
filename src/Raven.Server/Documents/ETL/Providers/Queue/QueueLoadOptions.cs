using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueLoadOptions
{
    public static HashSet<string> ValidOptions = new()
    {
        nameof(Id), nameof(Type), nameof(Source), nameof(PartitionKey), nameof(Exchange)
    };

    public string Id { get; set; }

    public string Type { get; set; }

    public string Source { get; set; }

    public string PartitionKey { get; set; }

    public string Exchange { get; set; }
}
