using System;
using System.Net;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Security.Windows
{
    public class WindowsAuthorizeRequests : AbstractAuthorizeRequests
    {
        public override bool Authorize(IHttpContext ctx)
        {
            if (Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.None && IsInvalidUser(ctx))
            {
                ctx.SetStatusToUnauthorized();
                return false;
            }

            IHttpRequest httpRequest = ctx.Request;

            if (Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
                IsInvalidUser(ctx) &&
                IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
            {
                ctx.SetStatusToUnauthorized();
                return false;
            }

            return true;
        }

        protected bool IsGetRequest(string httpMethod, string requestPath)
        {
        	return (httpMethod == "GET" || httpMethod == "HEAD") ||
        	       httpMethod == "POST" && (requestPath == "/multi_get/" || requestPath == "/multi_get");
        }

        private static bool IsInvalidUser(IHttpContext ctx)
        {
            return (ctx.User == null || ctx.User.Identity == null || ctx.User.Identity.IsAuthenticated == false);
        }
    }
}