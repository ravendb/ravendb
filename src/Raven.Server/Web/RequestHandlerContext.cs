using Microsoft.AspNet.Http;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public class RequestHandlerContext
    {
        public HttpContext HttpContext;
        public RavenServer RavenServer;
        public RouteMatch RouteMatch;
        public bool AllowResponseCompression;
        public DocumentDatabase Database;
    }
}