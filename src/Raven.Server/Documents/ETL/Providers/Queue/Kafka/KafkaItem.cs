namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

public sealed class KafkaItem : QueueItem
{
    public KafkaItem(QueueItem item) : base(item)
    {
    }
}
