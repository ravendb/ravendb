using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Connection;

namespace Raven.Client.Counters.Replication
{
	public interface ICountersReplicationInformer : IReplicationInformerBase<CountersClient>
	{
		Task UpdateReplicationInformationIfNeededAsync();

		//TODO: finish refactoring and remove this overload
		Task ExecuteWithReplicationAsync<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation);


		Task ExecuteWithReplicationAsync<T>(HttpMethod method, Func<OperationMetadata, Task<T>> operation);

		
		//TODO: finish refactoring and remove this overload
		Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation);


		Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, Func<OperationMetadata, Task<T>> operation);

		double MaxIntervalBetweenUpdatesInMillisec { get; set; }
	}
}
