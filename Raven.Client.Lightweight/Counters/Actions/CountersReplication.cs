using System;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	public class ReplicationClient : CountersActionsBase
	{
		internal ReplicationClient(CountersClient parent) : base(parent)
		{
		}

		public async Task<CounterStorageReplicationDocument> GetReplications()
		{
			var requestUriString = String.Format("{0}/replications/get", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync();
				return response.ToObject<CounterStorageReplicationDocument>(jsonSerializer);
			}
		}

		public async Task SaveReplications(CounterStorageReplicationDocument newReplicationDocument)
		{
			var requestUriString = String.Format("{0}/replications/save", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
			{
				await request.WriteAsync(RavenJObject.FromObject(newReplicationDocument));
				await request.ReadResponseJsonAsync();			
			}
		}
	}
}