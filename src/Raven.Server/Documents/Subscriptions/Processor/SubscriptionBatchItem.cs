using System;

namespace Raven.Server.Documents.Subscriptions.Processor;

public class SubscriptionBatchItem
{
    public Document Document;
    public Exception Exception;
    public SubscriptionBatchItemStatus Status;
}
