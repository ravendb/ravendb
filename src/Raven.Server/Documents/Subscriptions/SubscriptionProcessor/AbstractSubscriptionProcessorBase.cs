using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public abstract class AbstractSubscriptionProcessorBase : IDisposable
{
    public class BatchItem
    {
        public Document Document;
        public Exception Exception;
        public BatchItemStatus Status;
    }

    public class SubscriptionBatchResult
    {
        public string LastChangeVectorSentInThisBatch;
        public List<BatchItem> CurrentBatch;
        public BatchStatus Status;
    }

    public enum BatchStatus
    {
        EmptyBatch,
        DocumentsSent,
        ActiveMigration
    }

    public enum BatchItemStatus
    {
        Send,
        Skip,
        ActiveMigration
    }

    public virtual void Dispose()
    {
    }
}
