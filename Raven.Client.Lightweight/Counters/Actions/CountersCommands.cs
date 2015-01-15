using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
	public class CountersCommands
	{
		private readonly OperationCredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly string counterStorageUrl;
		private readonly CountersClient _parent;
		private readonly Convention convention;

		public CountersCommands(CountersClient parent, Convention convention)
		{
			credentials = parent.PrimaryCredentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			counterStorageUrl = parent.CounterStorageUrl;
			this._parent = parent;
			this.convention = convention;
		}

		public ProfilingInformation ProfilingInformation { get; private set; }

		public async Task Change(string group, string counterName, long delta)
		{
			var requestUriString = String.Format("{0}/change?group={1}&counterName={2}&delta={3}",
				counterStorageUrl, @group, counterName, delta);

			using (var request = _parent.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "POST", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public async Task Reset(string group, string counterName)
		{
			var requestUriString = String.Format("{0}/change?group={1}&counterName={2}",
				counterStorageUrl, @group, counterName);

			using (var request = _parent.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "POST", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public void Increment(string group, string counterName)
		{
			Change(@group, counterName, 1).Wait();
		}

		public void Decrement(string group, string counterName)
		{
			Change(@group, counterName, -1).Wait();
		}

		public async Task<long> GetOverallTotal(string group, string counterName)
		{
			var requestUriString = String.Format("{0}/getCounterOverallTotal?group={1}&counterName={2}",
				counterStorageUrl, @group, counterName);

			using (var request = _parent.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
					return response.Value<long>();
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public async Task<List<CounterView.ServerValue>> GetServersValues(string group, string counterName)
		{
			var requestUriString = String.Format("{0}/getCounterServersValues?group={1}&counterName={2}",
				counterStorageUrl, @group, counterName);

			using (var request = _parent.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
					return response.Value<List<CounterView.ServerValue>>();
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public CounterBatch CreateBatch()
		{
			return new CounterBatch(_parent, convention);
		}

		public class CounterBatch : IHoldProfilingInformation
		{
			private readonly OperationCredentials credentials;
			private readonly HttpJsonRequestFactory jsonRequestFactory;
			private readonly string counterStorageUrl;
			private readonly Convention convention;

			private readonly ConcurrentDictionary<string, long> counterData = new ConcurrentDictionary<string, long>();

			public CounterBatch(CountersClient countersClient, Convention convention)
			{
				credentials = countersClient.PrimaryCredentials;
				jsonRequestFactory = countersClient.JsonRequestFactory;
				counterStorageUrl = countersClient.CounterStorageUrl;
				this.convention = convention;
			}

			public ProfilingInformation ProfilingInformation { get; private set; }

			public void Change(string group, string counterName, long delta)
			{
				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { @group, counterName });

				counterData.AddOrUpdate(counterFullName, delta, (key, existingVal) => existingVal + delta);
			}

			public void Increment(string group, string counterName)
			{
				Change(@group, counterName, 1);
			}

			public void Decrement(string group, string counterName)
			{
				Change(@group, counterName, -1);
			}

			public async Task Write()
			{
				var counterChanges = new List<CounterChanges>();
				counterData.ForEach(keyValue =>
				{
					var newCounterChange = 
						new CounterChanges
						{
							FullCounterName = keyValue.Key,
							Delta = keyValue.Value
						};
					counterChanges.Add(newCounterChange);
				});

				var requestUriString = String.Format("{0}/batch", counterStorageUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)))
				{
					try
					{
						await request.WriteAsync(RavenJObject.FromObject(counterChanges));
						var response = await request.ReadResponseJsonAsync();
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
}