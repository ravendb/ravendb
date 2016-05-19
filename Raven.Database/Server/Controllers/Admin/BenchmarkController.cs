using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Server.WebApi.Attributes;


namespace Raven.Database.Server.Controllers.Admin
{
    public class BenchmarkController : BaseDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("Benchmark/EmptyMessage")]
        public HttpResponseMessage EmptyMessageTest()
        {
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
    }
}
