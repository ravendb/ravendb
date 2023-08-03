using System.Threading.Tasks;
using Raven.Client;

namespace Raven.Server.Web
{
    public abstract class ServerRequestHandler : RequestHandler
    {
        public override Task CheckForChanges(RequestHandlerContext context)
        {
            if (context.CheckForChanges == false)
                return Task.CompletedTask;

            var topologyEtag = GetLongFromHeaders(Constants.Headers.ClusterTopologyEtag);
            if (topologyEtag.HasValue && Server.ServerStore.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            return Task.CompletedTask;
        }
    }
}
