using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class SimpleTestController : ApiController
    {
        [HttpGet][Route("simples")]
        public HttpResponseMessage SimpleGet()
        {
            return new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new JsonContent(new RavenJValue("twat"))
            };
        }
    }
}