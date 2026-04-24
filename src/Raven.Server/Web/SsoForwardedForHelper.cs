using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;

namespace Raven.Server.Web;

internal static class SsoForwardedForHelper
{
    public static (string ClientIp, string ProxyIp) GetIps(HttpContext context, RavenServer.AuthenticateConnection auth)
    {
        var directIp = context.Connection.RemoteIpAddress?.ToString();

        // Only trust X-Forwarded-For from SSO-authenticated connections
        if (auth?.IsSsoAuthenticated != true)
            return (directIp, null);

        if (context.Request.Headers.TryGetValue("X-Forwarded-For", out var xff))
        {
            // Return the full X-Forwarded-For chain as ClientIp (all hops),
            // and the direct connection IP as ProxyIp (the SSO server).
            // Example: "10.0.0.5, 10.0.1.1" means client -> intermediate -> SSO -> RavenDB
            var forwardedFor = xff.ToString().Trim();
            if (string.IsNullOrEmpty(forwardedFor) == false)
                return (forwardedFor, directIp);
        }

        return (directIp, null);
    }
}
