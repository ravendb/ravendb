using System.Text;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;

namespace Raven.Server.Documents.Sharding
{
    public class ShardedRequestHandler : RequestHandler
    {
        public ShardedContext ShardedContext;
        public TransactionContextPool ContextPool;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            ShardedContext = context.ShardedContext;
            //TODO: We probably want to put it in the ShardedContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;

            
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && ShardedContext.HasTopologyChanged(topologyEtag.Value))
            {
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";
            }

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ShardedContext.HasClientConfigurationChanged(clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";
            
        }
    }
}
