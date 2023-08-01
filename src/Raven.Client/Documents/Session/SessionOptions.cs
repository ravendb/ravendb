using Raven.Client.Http;

namespace Raven.Client.Documents.Session
{
    public enum TransactionMode
    {
        SingleNode,
        ClusterWide,
    }

    public enum ShardedBatchBehavior
    {
        /// <summary>
        /// Do not force any behavior from the Client API and rely on Server's default
        /// </summary>
        Default,

        /// <summary>
        /// Allow to perform batch commands only on a single bucket, commands will be performed on single shard with ACID transaction guarantees.
        /// A transaction that contains changes that belong to multiple buckets will be rejected by the server.
        /// </summary>
        TransactionalSingleBucketOnly,

        /// <summary>
        /// Allow to spread batch commands to multiple buckets, commands can be performed on multiple shards without ACID transaction guarantees
        /// </summary>
        NonTransactionalMultiBucket
    }

    public sealed class SessionOptions
    {
        public string Database { get; set; }

        public bool NoTracking { get; set; }

        public bool NoCaching { get; set; }

        public RequestExecutor RequestExecutor { get; set; }

        /// <summary>
        /// Once TransactionMode set 'ClusterWide' it  will perform the SaveChanges as a transactional cluster wide operation.
        /// Any document store or delete will be part of this session's cluster transaction.
        /// </summary>
        public TransactionMode TransactionMode { get; set; }

        /// <summary>
        ///EXPERT: Disable automatic atomic writes with cluster write transactions. If set to 'true',
        /// will only consider explicitly added compare exchange values to validate cluster wide transactions."
        /// </summary>
        public bool? DisableAtomicDocumentWritesInClusterWideTransaction { get; set; }

        public ShardedBatchBehavior? ShardedBatchBehavior { get; set; }
    }
}
