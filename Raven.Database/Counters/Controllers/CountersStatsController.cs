using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Counters;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Counters.Controllers
{
	public class CountersStatsController : BaseCountersApiController
    {
		[RavenRoute("cs/{counterStorageName}/stats")]
		[HttpGet]
		public HttpResponseMessage CounterStats()
		{
			return GetMessageWithObject(CounterStorage.CreateStats());
		}

		[RavenRoute("cs/{counterStorageName}/metrics")]
		[HttpGet]
		public HttpResponseMessage CounterMetrics()
		{
			return GetMessageWithObject(CounterStorage.CreateMetrics());
		}

		[RavenRoute("cs/{counterStorageName}/replications/stats")]
		[HttpGet]
		public HttpResponseMessage ReplicationStats()
		{
			return GetMessageWithObject(new CounterStorageReplicationStats
				{
					Stats = CounterStorage.ReplicationTask.DestinationStats.Values.ToList()
				});
		}
    }
}
