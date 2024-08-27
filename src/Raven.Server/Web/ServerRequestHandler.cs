using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
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

        protected void AssertCanPersistConfiguration()
        {
            var authenticateConnection = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            if (authenticateConnection != null && authenticateConnection.Status != RavenServer.AuthenticationStatus.ClusterAdmin)
            {
                throw new UnauthorizedAccessException($"Configuration was modified but couldn't be persistent because the authentication level is {authenticateConnection.Status}, " +
                                                      $"but can be only executed with authentication level of {nameof(RavenServer.AuthenticationStatus.ClusterAdmin)}");
            }
        }
    }
}
