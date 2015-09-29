// -----------------------------------------------------------------------
//  <copyright file="ReplicationTopologyController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Counters.Replication;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public class CountersReplicationTopologyController : BaseCountersApiController
	{
		[HttpPost]
		[RavenRoute("cs/{counterStorageName}/admin/replication/topology/discover")]
		public async Task<HttpResponseMessage> ReplicationTopologyDiscover()
		{
			var ttlAsString = GetQueryStringValue("ttl");

			int ttl;
			RavenJArray from;

			if (string.IsNullOrEmpty(ttlAsString))
			{
				ttl = 10;
				from = new RavenJArray();
			}
			else
			{
				ttl = int.Parse(ttlAsString);
				from = await ReadJsonArrayAsync().ConfigureAwait(false);
			}

			var replicationSchemaDiscoverer = new CountersReplicationTopologyDiscoverer(CounterStorage, from, ttl, Log);
			var node = replicationSchemaDiscoverer.Discover();

			return GetMessageWithObject(node);
		}
	}
}