using System;
using static Raven.Server.Documents.Subscriptions.SubscriptionFetcher;

namespace Raven.Server.Documents.Subscriptions.Processor;

public sealed class SubscriptionBatchItem
{
    public Document Document;
    public Exception Exception;
    public SubscriptionBatchItemStatus Status;
    public FetchingOrigin FetchingFrom;
}
