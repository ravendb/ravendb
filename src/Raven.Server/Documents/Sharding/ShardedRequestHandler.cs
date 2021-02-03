using System;
using System.Net.Http;
using Raven.Client;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding
{
    public class ShardedRequestHandler : RequestHandler
    {
        public ShardedContext ShardedContext;
        public TransactionContextPool ContextPool;

        public HttpMethod Method;
        public string RelativeShardUrl;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            ShardedContext = context.ShardedContext;
            //TODO - sharding: We probably want to put it in the ShardedContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;

            
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && ShardedContext.HasTopologyChanged(topologyEtag.Value))
            {
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";
            }

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ShardedContext.HasClientConfigurationChanged(clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            var request = HttpContext.Request;
            var url = request.Path.Value;
            var relativeIndex = url.IndexOf('/', 11); //start after "/databases/" and skip the database name

            RelativeShardUrl = url.Substring(relativeIndex) + request.QueryString;
            Method = new HttpMethod(request.Method);
        }

        public void AddHeaders<T>(ShardedBaseCommand<T> command, Headers header)
        {
            if (header == Headers.None)
                return;

            if (header.HasFlag(Headers.IfMatch))
                command.Headers["If-Match"] = GetStringFromHeaders("If-Match");

            if (header.HasFlag(Headers.IfNonMatch))
                command.Headers["If-None-Match"] = GetStringFromHeaders("If-None-Match");
        }
    }
}
