using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Commands.Batches
{
    public class BatchOptions
    {
        public TimeSpan? RequestTimeout { get; set; }
        public ReplicationBatchOptions ReplicationOptions { get; set; }
        public IndexBatchOptions IndexOptions { get; set; }
        public ShardedBatchOptions ShardedOptions { get; set; }
    }

    public class IndexBatchOptions
    {
        public bool WaitForIndexes { get; set; }
        public TimeSpan WaitForIndexesTimeout { get; set; }
        public bool ThrowOnTimeoutInWaitForIndexes { get; set; }
        public string[] WaitForSpecificIndexes { get; set; }
    }

    public class ReplicationBatchOptions
    {
        public bool WaitForReplicas { get; set; }
        public int NumberOfReplicasToWaitFor { get; set; }
        public TimeSpan WaitForReplicasTimeout { get; set; }
        public bool Majority { get; set; }
        public bool ThrowOnTimeoutInWaitForReplicas { get; set; }
    }

    public class ShardedBatchOptions
    {
        internal static readonly ShardedBatchOptions MultiBucket = new() { BatchBehavior = ShardedBatchBehavior.MultiBucket };

        internal static readonly ShardedBatchOptions SingleBucket = new() { BatchBehavior = ShardedBatchBehavior.SingleBucket };

        public ShardedBatchBehavior BatchBehavior { get; set; }

        internal static ShardedBatchOptions For(ShardedBatchBehavior behavior)
        {
            return behavior switch
            {
                ShardedBatchBehavior.Default => null,
                ShardedBatchBehavior.SingleBucket => SingleBucket,
                ShardedBatchBehavior.MultiBucket => MultiBucket,
                _ => throw new ArgumentOutOfRangeException(nameof(behavior), behavior, null)
            };
        }
    }
}
