using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection.Async;

namespace Raven.Client.Connection
{
    public interface IDocumentStoreReplicationInformer : IReplicationInformerBase<ServerClient>
    {
        /// <summary>
        /// Failover servers set manually in config file or when document store was initialized
        /// </summary>
        ReplicationDestination[] FailoverServers { get; set; }

        /// <summary>
        ///  Updates replication information if needed
        /// </summary>
        /// <param name="serverClient"> DatabaseCommand to operate on</param>
        /// <param name="force">If set to true will fetch the topology regardless of the last update time</param>
        /// <returns></returns>
        Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient, bool force = false);

        /// <summary>
        /// Updates replication information from given topology document
        /// </summary>
        /// <param name="document"></param>
        void UpdateReplicationInformationFromDocument(JsonDocument document);
    }
}
