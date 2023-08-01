namespace Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;

public sealed class RabbitMqItem : QueueItem
{
    public RabbitMqItem(QueueItem item) : base(item)
    {
    }

    public string RoutingKey { get; set; }
}
