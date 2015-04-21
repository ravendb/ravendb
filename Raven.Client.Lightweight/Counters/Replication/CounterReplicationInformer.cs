using System;
using System.Linq;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Connection.Request;
using Raven.Client.Counters.Actions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
	public class CounterReplicationInformer : ReplicationInformerBase<CountersClient>,ICountersReplicationInformer
	{

		public CounterReplicationInformer(Client.Convention conventions, HttpJsonRequestFactory requestFactory, int delayTime = 1000) : base(conventions, requestFactory, delayTime)
		{
		}

		public override void RefreshReplicationInformation(CountersClient client)
		{
			JsonDocument document;
			var serverHash = ServerHash.GetServerHash(client.ServerUrl);
			try
			{
				var replicationFetchTask = client.Replication.GetReplicationsAsync();
				FailureCounters.FailureCounts[client.ServerUrl] = new FailureCounter(); // we just hit the master, so we can reset its failure count
				replicationFetchTask.Wait();

				document = new JsonDocument
				{
					DataAsJson = RavenJObject.FromObject(replicationFetchTask.Result)
				};
			}
			catch (Exception e)
			{
				Log.ErrorException("Could not contact master for fetching replication information",e);
				document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);

			}

			if (document == null || document.DataAsJson == null)
				return;

			ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

			UpdateReplicationInformationFromDocument(document);
		}

		public override void ClearReplicationInformationLocalCache(CountersClient client)
		{
			var serverHash = ServerHash.GetServerHash(client.ServerUrl);
			ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
		}

		protected override void UpdateReplicationInformationFromDocument(JsonDocument document)
		{
			var destinations = document.DataAsJson.Value<RavenJArray>("Destinations").Select(x => JsonConvert.DeserializeObject<CounterReplicationDestination>(x.ToString()));

			ReplicationDestinations = destinations.Select(x =>
			{
				ICredentials credentials = null;
				if (string.IsNullOrEmpty(x.Username) == false)
				{
					credentials = string.IsNullOrEmpty(x.Domain)
									  ? new NetworkCredential(x.Username, x.Password)
									  : new NetworkCredential(x.Username, x.Password, x.Domain);
				}

				return new OperationMetadata(x.ServerUrl, new OperationCredentials(x.ApiKey, credentials), null);
			})
				// filter out replication destination that don't have the url setup, we don't know how to reach them
				// so we might as well ignore them. Probably private replication destination (using connection string names only)
			.Where(x => x != null)
			.ToList();

			foreach (var replicationDestination in ReplicationDestinations)
			{
				FailureCounter value;
				if (FailureCounters.FailureCounts.TryGetValue(replicationDestination.Url, out value))
					continue;
				FailureCounters.FailureCounts[replicationDestination.Url] = new FailureCounter();
			}
		}

		//cs/{counterStorageName}/replication/heartbeat
		protected override string GetServerCheckUrl(string baseUrl)
		{
			return baseUrl + "/replication/heartbeat";
		}
	}
}
