namespace Raven.Server.Documents.ETL.Providers.Queue.AmazonSqs;

public sealed class AmazonSqsItem : QueueItem
{
    public AmazonSqsItem(QueueItem item) : base(item)
    {
    }
}
