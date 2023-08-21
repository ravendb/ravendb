namespace Raven.Client.Documents.Subscriptions;

public enum SubscriptionSourceItemKind
{
    Default, // only unarchived documents will be processed by the index
    ArchivedOnly, // only archived docs will be processed by the index
    ArchivedIncluded // all documents will be processed by the index

}
