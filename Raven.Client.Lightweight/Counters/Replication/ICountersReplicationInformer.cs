using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Connection;

namespace Raven.Client.Counters.Replication
{
	public interface ICountersReplicationInformer : IReplicationInformerBase<CountersClient>
	{
		Task UpdateReplicationInformationIfNeededAsync(CountersClient client);

		Task ExecuteWithReplicationAsync<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation);
		Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation);
		double MaxIntervalBetweenUpdatesInMillisec { get; set; }
	}
}
