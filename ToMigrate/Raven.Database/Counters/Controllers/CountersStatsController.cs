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
        public HttpResponseMessage ReplicationStats(int skip, int take)
        {
            if(take == 0)
                return GetMessageWithObject(new
                {
                    Message = "Take parameter is missing or zero. It is a required operator and should have non-zero value."
                },HttpStatusCode.BadRequest);

            return GetMessageWithObject(new CounterStorageReplicationStats
                {
                    Stats = CounterStorage.ReplicationTask.DestinationStats.Values.Skip(skip).Take(take).ToList()
                });
        }
    }
}
