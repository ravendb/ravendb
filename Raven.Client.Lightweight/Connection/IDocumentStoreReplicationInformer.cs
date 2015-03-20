using System.Threading.Tasks;

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
		/// Updates replication information if needed
		/// </summary>
		Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient);
	}
}