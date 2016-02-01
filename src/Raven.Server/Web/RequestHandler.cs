using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RequestHandler).FullName);

        protected HttpContext HttpContext;
        public ServerStore ServerStore;
        public RouteMatch RouteMatch;

        public virtual void Init(RequestHandlerContext context)
        {
            HttpContext = context.HttpContext;
            ServerStore = context.ServerStore;
            RouteMatch = context.RouteMatch;
        }
    }
}