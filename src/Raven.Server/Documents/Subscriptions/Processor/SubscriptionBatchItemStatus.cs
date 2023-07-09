namespace Raven.Server.Documents.Subscriptions.Processor;

public enum SubscriptionBatchItemStatus
{
    Send,
    Skip,
    ActiveMigration,
    Exception
}
