using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Security.Windows
{
    public class WindowsRequestAuthorizer : AbstractRequestAuthorizer
    {
        public override bool Authorize(IHttpContext ctx)
        {
            if (server.DefaultConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && IsInvalidUser(ctx))
            {
                ctx.SetStatusToUnauthorized();
                return false;
            }

            IHttpRequest httpRequest = ctx.Request;

			if (server.DefaultConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
                IsInvalidUser(ctx) &&
                IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
            {
                ctx.SetStatusToUnauthorized();
                return false;
            }

            return true;
        }

      

        private static bool IsInvalidUser(IHttpContext ctx)
        {
            return (ctx.User == null || ctx.User.Identity == null || ctx.User.Identity.IsAuthenticated == false);
        }
    }
}