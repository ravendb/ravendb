using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Raven.Database.Server.Connections;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
    public class HttpTraceFsController : RavenFsApiController
    {
        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/traffic-watch/events")]
        public HttpResponseMessage HttpTrace()
        {
            var traceTransport = new HttpTracePushContent(this);
            traceTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");

            RequestManager.RegisterResourceHttpTraceTransport(traceTransport, FileSystemName);

            return new HttpResponseMessage { Content = traceTransport };
        }
    }
}
