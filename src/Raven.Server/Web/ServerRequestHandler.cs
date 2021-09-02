using System.Threading.Tasks;
using Raven.Client;

namespace Raven.Server.Web
{
    public abstract class ServerRequestHandler : RequestHandler
    {
        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            context.HttpContext.Response.OnStarting(() => CheckForTopologyChanges(context));
        }

        public Task CheckForTopologyChanges(RequestHandlerContext context)
        {
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && Server.ServerStore.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            return Task.CompletedTask;
        }
    }
}
