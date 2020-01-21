using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Web
{
    public class RequestHandlerContext
    {
        public HttpContext HttpContext;
        public RavenServer RavenServer;
        public RouteMatch RouteMatch;
        public DocumentDatabase Database;
        public ShardedContext ShardedContext;
    }
}
