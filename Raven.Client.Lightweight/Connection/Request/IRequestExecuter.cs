using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Replication;

namespace Raven.Client.Connection.Request
{
	public interface IRequestExecuter
	{
		ReplicationDestination[] FailoverServers { get; set; }

		Task<T> ExecuteOperationAsync<T>(string method, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token);

		Task UpdateReplicationInformationIfNeeded(bool force = false);

		HttpJsonRequest AddHeaders(HttpJsonRequest httpJsonRequest);
	}
}