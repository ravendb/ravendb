using Microsoft.AspNet.Http;
using Raven.Client.Document;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public class RequestHandlerContext
    {
        public HttpContext HttpContext;
        public ServerStore ServerStore;
        public RouteMatch RouteMatch;

        public DocumentsStorage DocumentsStorage;
    }
}