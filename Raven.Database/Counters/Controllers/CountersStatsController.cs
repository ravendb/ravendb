using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Counters;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Counters.Controllers
{
	public class CountersStatsController : RavenCountersApiController
    {
		[RavenRoute("cs/{counterStorageName}/stats")]
		[HttpGet]
		public HttpResponseMessage CounterStats()
		{
			return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateStats());
		}

		[RavenRoute("cs/{counterStorageName}/metrics")]
		[HttpGet]
		public HttpResponseMessage CounterMetrics()
		{
			return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateMetrics());
		}

		[RavenRoute("cs/{counterStorageName}/replications/stats")]
		[HttpGet]
		public HttpResponseMessage ReplicationStats()
		{
			return Request.CreateResponse(HttpStatusCode.OK,
				new CounterStorageReplicationStats()
				{
					Stats = Storage.ReplicationTask.DestinationStats.Values.ToList()
				});
		}
    }
}
