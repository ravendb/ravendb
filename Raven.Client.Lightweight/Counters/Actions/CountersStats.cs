using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;

namespace Raven.Client.Counters.Actions
{
	public class CountersStats : CountersActionsBase
	{
		internal CountersStats(CountersClient parent) : base(parent)
		{
		}

		public async Task<List<CounterStorageStats>> GetCounterStorageStats()
		{
			var requestUriString = String.Format("{0}/stats", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString,Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync();
				return response.ToObject<List<CounterStorageStats>>(jsonSerializer);
			}
		}

		public async Task<List<CountersStorageMetrics>> GetCounterStorageMetrics()
		{
			var requestUriString = String.Format("{0}/metrics", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync();
				return response.ToObject<List<CountersStorageMetrics>>(jsonSerializer);
			}
		}

		public async Task<List<CounterStorageReplicationStats>> GetCounterStoragRelicationStats()
		{
			var requestUriString = String.Format("{0}/replications/stats", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync();
				return response.ToObject<List<CounterStorageReplicationStats>>(jsonSerializer);
			}
		}
	}
}