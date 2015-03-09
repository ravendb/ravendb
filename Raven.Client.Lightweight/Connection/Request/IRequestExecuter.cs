using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Connection.Request
{
	public interface IRequestExecuter
	{
		Task<T> ExecuteOperationAsync<T>(string method, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token);

		Task UpdateReplicationInformationIfNeeded();
	}
}