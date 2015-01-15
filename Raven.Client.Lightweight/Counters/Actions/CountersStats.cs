using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	public class CountersStats
	{
		private readonly OperationCredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly string counterStorageUrl;
		private readonly CountersClient _parent;
		private readonly Convention convention;

		public CountersStats(CountersClient parent, Convention convention)
		{
			credentials = parent.PrimaryCredentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			counterStorageUrl = parent.CounterStorageUrl;
			_parent = parent;
			this.convention = convention;
		}

		public ProfilingInformation ProfilingInformation { get; private set; }

		public async Task<List<CounterStorageStats>> GetCounterStorageStats()
		{
			var requestUriString = String.Format("{0}/stats", counterStorageUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
					return new JsonSerializer().Deserialize<List<CounterStorageStats>>(new RavenJTokenReader(response));
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public async Task<List<CountersStorageMetrics>> GetCounterStorageMetrics()
		{
			var requestUriString = String.Format("{0}/metrics", counterStorageUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
					return new JsonSerializer().Deserialize<List<CountersStorageMetrics>>(new RavenJTokenReader(response));
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public async Task<List<CounterStorageReplicationStats>> GetCounterStoragRelicationStats()
		{
			var requestUriString = String.Format("{0}/replications/stats", counterStorageUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
					return new JsonSerializer().Deserialize<List<CounterStorageReplicationStats>>(new RavenJTokenReader(response));
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}
	}
}