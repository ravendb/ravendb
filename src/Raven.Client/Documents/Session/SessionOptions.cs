using System;
using Raven.Client.Http;

namespace Raven.Client.Documents.Session
{
    public enum TransactionMode
    {
        /// <summary>
        /// Calling <see cref="DocumentSession.SaveChanges"/> will persist all modifications to a node.<br/>
        /// This is the default mode to favor performance and availability
        /// </summary>
        /// <remarks>For more details visit: <inheritdoc cref="DocumentationUrls.Session.Transactions.TransactionSupport"/></remarks>

        SingleNode,
        /// <summary>
        /// Calling <see cref="DocumentSession.SaveChanges"/> will persist all modifications consistently across the entire cluster.<br/>
        /// This mode uses RAFT to ensure consistency in the cluster and require the majority of the nodes to be available.<br/>
        /// </summary>
        /// <remarks>For more details visit: <inheritdoc cref="DocumentationUrls.Session.Transactions.TransactionSupport"/></remarks>
        ClusterWide,
    }

    public enum ShardedBatchBehavior
    {
        /// <summary>
        /// Do not force any behavior from the Client API and rely on Server's default
        /// </summary>
        Default,

        /// <summary>
        /// Allow to perform batch commands only on a single bucket, commands will be performed on single shard with <see cref="TransactionMode.SingleNode"/> transaction guarantees.
        /// A transaction that contains changes that belong to multiple buckets will be rejected by the server
        /// and <see cref="Exceptions.Sharding.ShardedBatchBehaviorViolationException"/> will be thrown.
        /// </summary>
        /// <remarks>Check how you can force documents being in the same bucket by anchoring them:<br/> <inheritdoc cref="DocumentationUrls.Session.Sharding.Anchoring"/></remarks>
        TransactionalSingleBucketOnly,

        /// <summary>
        /// Allow to spread batch commands to multiple buckets, commands can be performed on multiple shards without ACID transaction guarantees
        /// </summary>
        NonTransactionalMultiBucket
    }

    public enum OptimisticConcurrencyMode
    {
        /// <summary>
        /// No optimistic concurrency checks are performed.<br/>
        /// During <see cref="DocumentSession.SaveChanges"/>, PUT and DELETE commands are sent without a change vector, so the server does not check for concurrent modifications.
        /// </summary>
        None,

        /// <summary>
        /// Optimistic concurrency checks are performed for written (PUT) and deleted (DELETE) entities only.<br/>
        /// Entities that are tracked by the session but were not modified/deleted are <b>not</b> checked.<br/>
        /// <br/>
        /// During <see cref="DocumentSession.SaveChanges"/>, each PUT/DELETE command includes the entity's change vector so the server rejects the save operation
        /// if the document was modified by another session since it was first tracked by the session.<br/>
        /// <br/>
        /// This mode is incompatible with <see cref="SessionOptions.NoTracking"/>
        /// and <see cref="TransactionMode.ClusterWide"/>.
        /// </summary>
        Writes,

        /// <summary>
        /// Optimistic concurrency checks are performed for <b>all</b> entities tracked by the session - both modified and not modified.<br/>
        /// <br/>
        /// During <see cref="DocumentSession.SaveChanges"/>, the session sends the change vector of <b>all</b> tracked documents to the server.
        /// The server rejects the save operation if any of these documents was modified by another session since it was first tracked by the session.<br/>
        /// <br/>
        /// This mode is incompatible with <see cref="SessionOptions.NoTracking"/>,
        /// <see cref="TransactionMode.ClusterWide"/>, and sharded databases.
        /// </summary>
        WritesAndReads
    }

    /// <summary>
    /// Configure the Session's behavior
    /// </summary>
    public sealed class SessionOptions
    {
        /// <summary>
        /// Specify session's database, default value is taken from <see cref="IDocumentStore.Database"/>.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// Disable tracking for all entities in the session<br/>
        /// </summary>
        /// <remarks>For more details visit: <inheritdoc cref="DocumentationUrls.Session.Options.NoTracking"/></remarks>
        public bool NoTracking
        {
            get;
            set
            {
                if (value && _optimisticConcurrencyMode != null && _optimisticConcurrencyMode != Session.OptimisticConcurrencyMode.None)
                {
                    throw new InvalidOperationException(
                        $"{nameof(NoTracking)} cannot be set to true when {nameof(OptimisticConcurrencyMode)} is {_optimisticConcurrencyMode}.");
                }

                field = value;
            }
        }

        /// <summary>
        /// Configure optimistic concurrency mode for the session.<br/>
        /// When set, overrides the default from <see cref="Conventions.DocumentConventions.OptimisticConcurrencyMode"/>.<br/>
        /// When <c>null</c> (default), the session inherits the value from conventions.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when set to <see cref="OptimisticConcurrencyMode.Writes"/> or
        /// <see cref="OptimisticConcurrencyMode.WritesAndReads"/> while
        /// <see cref="NoTracking"/> is <c>true</c> or <see cref="TransactionMode"/> is <see cref="TransactionMode.ClusterWide"/>.
        /// </exception>
        public OptimisticConcurrencyMode? OptimisticConcurrencyMode
        {
            get => _optimisticConcurrencyMode;
            set
            {
                if (value != null && value != Session.OptimisticConcurrencyMode.None
                    && TransactionMode == TransactionMode.ClusterWide)
                {
                    throw new InvalidOperationException(
                        $"{nameof(OptimisticConcurrencyMode)} cannot be set to {value} when {nameof(TransactionMode)} is {TransactionMode.ClusterWide}.");
                }

                if (value != null && value != Session.OptimisticConcurrencyMode.None && NoTracking)
                {
                    throw new InvalidOperationException(
                        $"{nameof(OptimisticConcurrencyMode)} cannot be set to {value} when {nameof(NoTracking)} is true.");
                }

                _optimisticConcurrencyMode = value;
            }
        }

        private OptimisticConcurrencyMode? _optimisticConcurrencyMode;

        /// <summary>
        /// Disable caching of HTTP responses for the session<br/>
        /// </summary>
        /// <remarks>For more details visit: <inheritdoc cref="DocumentationUrls.Session.Options.NoCaching"/></remarks>
        public bool NoCaching { get; set; }

        public RequestExecutor RequestExecutor { get; set; }

        /// <summary>
        /// Define the transaction mode of the session. <br/>
        /// Each <see cref="TransactionMode"/> offers a different isolation and consistency guarantees
        /// </summary>
        /// <remarks>For more details: <inheritdoc cref="DocumentationUrls.Session.Transactions.TransactionSupport"/></remarks>
        public TransactionMode TransactionMode
        {
            get;
            set
            {
                if (value == TransactionMode.ClusterWide &&
                    _optimisticConcurrencyMode != null && _optimisticConcurrencyMode != Session.OptimisticConcurrencyMode.None)
                {
                    throw new InvalidOperationException(
                        $"{nameof(OptimisticConcurrencyMode)} cannot be set to {_optimisticConcurrencyMode} when {nameof(TransactionMode)} is {TransactionMode.ClusterWide}.");
                }

                field = value;
            }
        }

        /// <summary>
        ///EXPERT: Disable automatic atomic writes with cluster write transactions. If set to 'true',
        /// will only consider explicitly added compare exchange values to validate cluster wide transactions."
        /// </summary>
        public bool? DisableAtomicDocumentWritesInClusterWideTransaction { get; set; }

        /// <summary>
        /// Define the consistency level for persisting changes in a sharded database.
        /// </summary>
        public ShardedBatchBehavior? ShardedBatchBehavior { get; set; }
    }
}
