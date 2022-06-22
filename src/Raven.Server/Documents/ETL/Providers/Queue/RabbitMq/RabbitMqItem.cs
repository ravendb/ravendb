namespace Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;

public class RabbitMqItem : QueueItem
{
    public RabbitMqItem(QueueItem item) : base(item)
    {
    }

    public string RoutingKey { get; set; }
}
