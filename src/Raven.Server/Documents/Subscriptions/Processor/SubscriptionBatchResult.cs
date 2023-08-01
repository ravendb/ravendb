using System.Collections.Generic;

namespace Raven.Server.Documents.Subscriptions.Processor;

public sealed class SubscriptionBatchResult
{
    public string LastChangeVectorSentInThisBatch;
    public List<SubscriptionBatchItem> CurrentBatch;
    public SubscriptionBatchStatus Status;
}
