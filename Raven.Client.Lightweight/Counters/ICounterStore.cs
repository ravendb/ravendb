using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Counters.Changes;
using Raven.Client.Counters.Replication;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters
{
	public interface ICounterStore : IHoldProfilingInformation, IDisposalNotification
	{
		
		OperationCredentials Credentials { get; set; }

		HttpJsonRequestFactory JsonRequestFactory { get; set; }

		string Url { get; }

		string DefaultCounterStorageName { get; }

		Convention Convention { get; set; }

		JsonSerializer JsonSerializer { get; set; }

		CounterStore.CounterStoreAdvancedOperations Advanced { get; }

		/// <summary>
		/// Create new counter storage on the server.
		/// </summary>
		/// <param name="counterStorageDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
		/// <param name="counterStorageName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
		Task CreateCounterStorageAsync(CounterStorageDocument counterStorageDocument, string counterStorageName, bool shouldUpateIfExists = false, CancellationToken token = default(CancellationToken));

		Task DeleteCounterStorageAsync(string counterStorageName, bool hardDelete = false, CancellationToken token = default(CancellationToken));
		
		Task<string[]> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken));

		CountersClient NewCounterClient(string counterStorageName = null);

		CounterStore.BatchOperationsStore Batch { get; }

		ICountersReplicationInformer ReplicationInformer { get; }

		void Initialize(bool ensureDefaultCounterExists = false);

		/// <summary>
		/// Subscribe to change notifications from the server
		/// </summary>
		ICountersChanges Changes(string counterStorage = null);
	}
}
