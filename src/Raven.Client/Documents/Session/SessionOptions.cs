using System;
using Raven.Client.Http;
#pragma warning disable CS0618 // Type or member is obsolete

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

    public enum TrackingMode
    {
        /// <summary>
        /// Do not force any behavior from the Client API and rely on Server's default
        /// </summary>
        Default,

        /// <summary>
        /// Disable tracking for all entities in the session<br/>
        /// </summary>
        NoTracking,

        /// <summary>
        /// Enable tracking for all entities in the session<br/>
        /// </summary>
        TrackAllEntities
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
        /// <remarks>For more details visit: <inheritdoc cref="DocumentationUrls.Session.Options.TrackingMode"/></remarks>
        [Obsolete("SessionOptions.NoTracking is obsolete and will be removed in the next major version. Please use " +
                  nameof(SessionOptions) + "." + nameof(TrackingMode) + " instead. " +
                  "See: https://ravendb.net/docs/article-page/latest/csharp/client-api/session/options#notracking")]
        public bool NoTracking
        {
            get => TrackingMode == TrackingMode.NoTracking;
            set
            {
                if (TrackingModeWasSet)
                    throw new InvalidOperationException($"{nameof(NoTracking)} cannot be set when {nameof(TrackingMode)} was set. Please use {nameof(TrackingMode)} instead of {nameof(NoTracking)}.");

                TrackingMode = value ? TrackingMode.NoTracking : TrackingMode.Default;
                NoTrackingWasSet = true;
            }
        }

        internal bool NoTrackingWasSet { get; set; }
        internal bool TrackingModeWasSet { get; set; }

        /// <summary>
        /// Enable tracking mode in the session<br/>
        /// </summary>
        /// <remarks>For more details visit: <inheritdoc cref="DocumentationUrls.Session.Options.TrackingMode"/></remarks>
        public TrackingMode TrackingMode
        {
            get;
            set
            {
                if (NoTrackingWasSet)
                    throw new InvalidOperationException($"{nameof(TrackingMode)} cannot be set when {nameof(NoTracking)} was set. Please use {nameof(TrackingMode)} instead of {nameof(NoTracking)}.");

                TrackingModeWasSet = true;

                if (value == TrackingMode.TrackAllEntities)
                {
                    if (TransactionMode == TransactionMode.ClusterWide)
                        throw new InvalidOperationException($"{nameof(TrackingMode)} cannot be set to {nameof(TrackingMode.TrackAllEntities)} when {nameof(TransactionMode)} is {TransactionMode.ClusterWide}.");
                }

                field = value;
            }
        }

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
                if (TrackingMode == TrackingMode.TrackAllEntities)
                    throw new InvalidOperationException($"{nameof(TrackingMode)} cannot be set to {nameof(TrackingMode.TrackAllEntities)} when {nameof(TransactionMode)} is {TransactionMode.ClusterWide}.");

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
