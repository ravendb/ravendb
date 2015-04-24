using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Counters.Actions;

namespace Raven.Client.Counters
{
	public interface ICountersReplicationInformer : IReplicationInformerBase<CountersClient>
	{
		Task ExecuteWithReplicationAsync<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation);
		Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation);
	}
}
