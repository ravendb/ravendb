using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Counters.Actions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters
{
	public interface ICounterStore : IHoldProfilingInformation
	{
		
		OperationCredentials Credentials { get; set; }

		HttpJsonRequestFactory JsonRequestFactory { get; set; }

		string Url { get; set; }

		string DefaultCounterName { get; set; }

		Convention Convention { get; set; }

		JsonSerializer JsonSerializer { get; set; }

		/// <summary>
		/// Create new counter storage on the server.
		/// </summary>
		/// <param name="countersDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
		/// <param name="counterName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
		Task CreateCounterAsync(CountersDocument countersDocument, string counterName, bool shouldUpateIfExists = true, CancellationToken token = default(CancellationToken));

		Task DeleteCounterStorageAsync(string counterName, bool hardDelete = false, CancellationToken token = default(CancellationToken));
		
		Task<string[]> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken));

		Task<List<CounterStorageStats>> GetCounterStoragesStatsAsync(CancellationToken token = default(CancellationToken));

		CountersBatchOperation BatchOperation(string counterName, CountersBatchOptions options);

		CountersClient NewCounterClient(string counterName);

		void Initialize(bool ensureDefaultCounterExists = false);
	}
}
