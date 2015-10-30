using System.Net.Http;
using System.Web.Http;

using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    public class StatisticsController : RavenDbApiController
    {
        [HttpGet]
        [RavenRoute("stats")]
        [RavenRoute("databases/{databaseName}/stats")]
        public HttpResponseMessage Get()
        {
            return GetMessageWithObject(Database.Statistics);
        }
    }
}
