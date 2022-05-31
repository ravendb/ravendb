namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueLoadOptions
{
    public string Id { get; set; }

    public string Type { get; set; }

    public string Source { get; set; }

    public string PartitionKey { get; set; }

    public string RoutingKey { get; set; }
}
