using Raven.Client;

namespace Raven.Server.Web
{
    public abstract class ServerRequestHandler : RequestHandler
    {
        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && Server.ServerStore.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";
        }
    }
}
