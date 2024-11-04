namespace Raven.Server.Documents.ETL.Providers.Queue.AwsSqs;

public sealed class AwsSqsItem : QueueItem
{
    public AwsSqsItem(QueueItem item) : base(item)
    {
    }
}
