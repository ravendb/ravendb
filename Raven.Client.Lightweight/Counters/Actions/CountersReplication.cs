using System;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	public class ReplicationClient : CountersActionsBase
	{
		internal ReplicationClient(CountersClient parent, Convention convention)
			: base(parent, convention)
		{
		}

		public async Task<CounterStorageReplicationDocument> GetReplications()
		{
			var requestUriString = String.Format("{0}/replications/get", counterStorageUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(parent, requestUriString, "GET", credentials, convention)))
			{
				try
				{
					var response = await request.ReadResponseJsonAsync();
					return response.Value<CounterStorageReplicationDocument>();
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}

		public async Task SaveReplications(CounterStorageReplicationDocument newReplicationDocument)
		{
			var requestUriString = String.Format("{0}/replications/save", counterStorageUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(parent, requestUriString, "POST", credentials, convention)))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(newReplicationDocument));
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