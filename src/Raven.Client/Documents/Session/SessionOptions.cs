using Raven.Client.Http;

namespace Raven.Client.Documents.Session
{
    public enum TransactionMode
    {
        SingleNode,
        ClusterWide,
    }

    public class SessionOptions
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
    }
}
