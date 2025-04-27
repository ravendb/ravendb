using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class BatchOptions
    {
        public TimeSpan? RequestTimeout { get; set; }
        public ReplicationBatchOptions ReplicationOptions { get; set; }
        public IndexBatchOptions IndexOptions { get; set; }
        public ShardedBatchOptions ShardedOptions { get; set; }
    }

    public sealed class IndexBatchOptions
    {
        public bool WaitForIndexes { get; set; }
        public TimeSpan WaitForIndexesTimeout { get; set; }
        public bool ThrowOnTimeoutInWaitForIndexes { get; set; }
        public string[] WaitForSpecificIndexes { get; set; }
    }

    public sealed class ReplicationBatchOptions
    {
        public bool WaitForReplicas { get; set; }
        public int NumberOfReplicasToWaitFor { get; set; }
        public TimeSpan WaitForReplicasTimeout { get; set; }
        public bool Majority { get; set; }
        public bool ThrowOnTimeoutInWaitForReplicas { get; set; }
    }

    public sealed class ShardedBatchOptions
    {
        internal static readonly ShardedBatchOptions NonTransactionalMultiBucket = new() { BatchBehavior = ShardedBatchBehavior.NonTransactionalMultiBucket };

        internal static readonly ShardedBatchOptions TransactionalSingleBucketOnly = new() { BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly };

        public ShardedBatchBehavior BatchBehavior { get; set; }

        internal static ShardedBatchOptions For(ShardedBatchBehavior behavior)
        {
            return behavior switch
            {
                ShardedBatchBehavior.Default => null,
                ShardedBatchBehavior.TransactionalSingleBucketOnly => TransactionalSingleBucketOnly,
                ShardedBatchBehavior.NonTransactionalMultiBucket => NonTransactionalMultiBucket,
                _ => throw new ArgumentOutOfRangeException(nameof(behavior), behavior, null)
            };
        }
    }
}
