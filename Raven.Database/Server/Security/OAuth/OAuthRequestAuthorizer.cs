using System;
using System.Security.Principal;
using System.Linq;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthRequestAuthorizer : AbstractRequestAuthorizer
	{
		readonly string[] neverSecretUrls = new[]
			{
				// allow to actually handle the authentication
				"/OAuth/AccessToken",
				// allow to get files that are static and are never secret, for example, the studio, the cross domain
				// policy and the fav icon
				"/",
				"/raven/studio.html",
				"/silverlight/Raven.Studio.xap",
				"/favicon.ico",
				"/clientaccesspolicy.xml",
				"/build/version",
			};

		public override bool Authorize(IHttpContext ctx)
		{
			var httpRequest = ctx.Request;

			var requestUrl = ctx.GetRequestUrl();
			
			if (neverSecretUrls.Contains(requestUrl, StringComparer.InvariantCultureIgnoreCase))
				return true;

			var isGetRequest = IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath);
			var allowUnauthenticatedUsers = // we need to auth even if we don't have to, for bundles that want the user 
				Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.All || 
			        Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
			        isGetRequest;
			

			var token = GetToken(ctx);
			
			if (token == null)
			{
				if (allowUnauthenticatedUsers)
					return true;
				WriteAuthorizationChallenge(ctx, 401, "invalid_request", "The access token is required");
				
				return false;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(Settings.OAuthTokenCertificate, token, out tokenBody))
			{
				if (allowUnauthenticatedUsers)
					return true;
				WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is invalid");

				return false;
			}

			if (tokenBody.IsExpired())
			{
				if (allowUnauthenticatedUsers)
					return true;
				WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is expired");

				return false;
			}

			if(!tokenBody.IsAuthorized(TenantId))
			{
				if (allowUnauthenticatedUsers)
					return true;

				WriteAuthorizationChallenge(ctx, 403, "insufficient_scope", "Not authorized for tenant " + TenantId);
	   
				return false;
			}

			if(tokenBody.ReadOnly && isGetRequest)
			{
				WriteAuthorizationChallenge(ctx, 403, "insufficient_scope", "Not authorized for writing to tenant " + TenantId);

				return false;
			}
			
			ctx.User = new OAuthPrincipal(tokenBody);
			CurrentOperationContext.Headers.Value["Raven-Authenticated-User"] = tokenBody.UserId;
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

	public class OAuthPrincipal : IPrincipal, IIdentity
	{
		private readonly AccessTokenBody tokenBody;

		public OAuthPrincipal(AccessTokenBody tokenBody)
		{
			this.tokenBody = tokenBody;
		}

		public bool IsInRole(string role)
		{
			return false;
		}

		public string[] AuthorizedDatabases
		{
			get { return tokenBody.AuthorizedDatabases; }
		}

		public IIdentity Identity
		{
			get { return this; }
		}

		public string Name
		{
			get { return tokenBody.UserId; }
		}

		public string AuthenticationType
		{
			get { return "OAuth"; }
		}

		public bool IsAuthenticated
		{
			get { return true; }
		}
	}
}