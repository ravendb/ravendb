using Microsoft.AspNetCore.Http;

using Raven.Server.Documents;
using Raven.Server.Files;
using Raven.Server.Routing;

namespace Raven.Server.Web
{
    public class RequestHandlerContext
    {
        public HttpContext HttpContext;
        public RavenServer RavenServer;
        public RouteMatch RouteMatch;
        public bool AllowResponseCompression;
        public DocumentDatabase Database;
        public FileSystem FileSystem;
    }
}