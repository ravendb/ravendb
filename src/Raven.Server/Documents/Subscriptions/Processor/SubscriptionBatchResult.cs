using System.Collections.Generic;

namespace Raven.Server.Documents.Subscriptions.Processor;

public class SubscriptionBatchResult
{
    public string LastChangeVectorSentInThisBatch;
    public List<SubscriptionBatchItem> CurrentBatch;
    public SubscriptionBatchStatus Status;
}
