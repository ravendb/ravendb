using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Web.TEMP_REMOVE_ME
{
    public class MakeStudioWorkForNowHandler : RequestHandler
    {
        [RavenAction("/databases/*/configuration/document$", "GET")]
        public Task FakeResponseForConfigurationDocument()
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return Task.CompletedTask;
        }
    }
}