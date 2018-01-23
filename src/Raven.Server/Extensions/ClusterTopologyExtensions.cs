using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.ServerWide;

namespace Raven.Server.Extensions
{
    public static class ClusterTopologyExtensions
    {
        public static void ReplaceCurrentNodeUrlWithClientRequestedNodeUrlIfNecessary(this ClusterTopology topology, ServerStore serverStore, HttpContext httpContext)
        {
            var currentNodeUrlAsSeenByTheClient = serverStore.GetNodeHttpServerUrl(httpContext.Request.GetClientRequestedNodeUrl());
            topology.ReplaceCurrentNodeUrlWithClientRequestedUrl(serverStore.NodeTag, currentNodeUrlAsSeenByTheClient);
        }
    }
}
