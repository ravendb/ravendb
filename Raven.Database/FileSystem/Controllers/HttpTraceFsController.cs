using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Raven.Database.Server.Connections;

namespace Raven.Database.FileSystem.Controllers
{
    public class HttpTraceFsController : RavenFsApiController
    {
        [HttpGet]
        [Route("fs/{fileSystemName}/traffic-watch/events")]
        public HttpResponseMessage HttpTrace()
        {
            var traceTransport = new HttpTracePushContent(this);
            traceTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");

            RequestManager.RegisterResourceHttpTraceTransport(traceTransport, FileSystemName);

            return new HttpResponseMessage { Content = traceTransport };
        }
    }
}
