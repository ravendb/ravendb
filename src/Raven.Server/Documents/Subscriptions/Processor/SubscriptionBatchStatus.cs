namespace Raven.Server.Documents.Subscriptions.Processor;

public enum SubscriptionBatchStatus
{
    EmptyBatch,
    DocumentsSent,
    ActiveMigration
}
