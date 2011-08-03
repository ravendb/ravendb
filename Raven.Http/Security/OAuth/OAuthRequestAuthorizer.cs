using System;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Security.OAuth
{
    public class OAuthRequestAuthorizer : AbstractRequestAuthorizer
    {
        public override bool Authorize(IHttpContext ctx)
        {
            var httpRequest = ctx.Request;

            if (ctx.Request.RawUrl.StartsWith("/OAuth/AccessToken", StringComparison.InvariantCultureIgnoreCase))
                return true;

            if (Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
                IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath))
                return true;

            var token = GetToken(ctx);
            
            if (token == null)
            {
                ctx.SetStatusToUnauthorized();
                WriteAuthorizationChallenge(ctx, "invalid_request", "The access token is required");
                
                return false;
            }

            AccessToken accessToken;
            AccessTokenBody tokenBody;
            if (!AccessToken.TryParse(token, out accessToken) || !accessToken.MatchesSignature(Settings.OAuthTokenCertificatePath) || !accessToken.TryParseBody(out tokenBody))
            {
                ctx.SetStatusToUnauthorized();
                WriteAuthorizationChallenge(ctx, "invalid_token", "The access token is invalid");

                return false;
            }

            if (tokenBody.IsExpired())
            {
                ctx.SetStatusToUnauthorized();
                WriteAuthorizationChallenge(ctx, "invalid_token", "The access token is expired");

                return false;
            }

            if(!tokenBody.IsAuthorized(TenantId))
            {
                ctx.SetStatusToForbidden();
                WriteAuthorizationChallenge(ctx, "insufficient_scope", "Not authorized for tenant " + TenantId);

                return false;
            }

            return true;
        }

        static string GetToken(IHttpContext ctx)
        {
            const string bearerPrefix = "Bearer ";

            var auth = CurrentOperationContext.Headers.Value["Authorization"];

            if (auth == null || auth.Length <= bearerPrefix.Length || !auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
                return null;

            var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);
            
            return token;
        }

        protected bool IsGetRequest(string httpMethod, string requestPath)
        {
            return (httpMethod == "GET" || httpMethod == "HEAD");
        }
        
        static void WriteAuthorizationChallenge(IHttpContext ctx, string error, string errorDescription)
        {
            ctx.Response.AddHeader("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription));
        }
    }
}