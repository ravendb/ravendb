using System;
using System.Security.Cryptography.X509Certificates;
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
				WriteAuthorizationChallenge(ctx, 401, "invalid_request", "The access token is required");
                
                return false;
            }

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(Settings.OAuthTokenCertificate, token, out tokenBody))
            {
				WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is invalid");

                return false;
            }

            if (tokenBody.IsExpired())
            {
				WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is expired");

                return false;
            }

            if(!tokenBody.IsAuthorized(TenantId))
            {
				WriteAuthorizationChallenge(ctx, 403, "insufficient_scope", "Not authorized for tenant " + TenantId);
       
                return false;
            }

            return true;
        }

        static string GetToken(IHttpContext ctx)
        {
            const string bearerPrefix = "Bearer ";

            var auth = ctx.Request.Headers["Authorization"];

            if (auth == null || auth.Length <= bearerPrefix.Length || !auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
                return null;

            var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);
            
            return token;
        }

        void WriteAuthorizationChallenge(IHttpContext ctx, int statusCode, string error, string errorDescription)
        {
			if (string.IsNullOrEmpty(Settings.OAuthTokenServer) == false)
			{
				ctx.Response.AddHeader("OAuth-Source", Settings.OAuthTokenServer);
			}
        	ctx.Response.StatusCode = statusCode;
            ctx.Response.AddHeader("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription));
        }
    }
}