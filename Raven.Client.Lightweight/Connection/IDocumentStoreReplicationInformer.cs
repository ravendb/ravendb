using Raven.Abstractions.Replication;

namespace Raven.Client.Connection
{
	public interface IDocumentStoreReplicationInformer : IReplicationInformerBase<ServerClient>
	{
		/// <summary>
		/// Failover servers set manually in config file or when document store was initialized
		/// </summary>
		ReplicationDestination[] FailoverServers { get; set; }
	}
}