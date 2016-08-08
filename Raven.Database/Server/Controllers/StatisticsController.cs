using System.Net.Http;
using System.Web.Http;

using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    public class StatisticsController : BaseDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("stats")]
        [RavenRoute("databases/{databaseName}/stats")]
        public HttpResponseMessage Get()
        {
            return GetMessageWithObject(Database.Statistics);
        }

        [HttpGet]
        [RavenRoute("reduced-database-stats")]
        [RavenRoute("databases/{databaseName}/reduced-database-stats")]
        public HttpResponseMessage ReducedDatabaseStats()
        {
            return GetMessageWithObject(Database.ReducedStatistics);
        }

        [HttpGet]
        [RavenRoute("indexes-stats")]
        [RavenRoute("databases/{databaseName}/indexes-stats")]
        public HttpResponseMessage IndexesStats()
        {
            return GetMessageWithObject(Database.IndexesStatistics);
        }
    }
}
