using System;
using System.Security.Principal;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthRequestAuthorizer : AbstractRequestAuthorizer
	{
		public override bool Authorize(IHttpContext ctx)
		{
			var httpRequest = ctx.Request;

			var requestUrl = ctx.GetRequestUrl();
			
			if (NeverSecret.Urls.Contains(requestUrl))
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

			var writeAccess = isGetRequest == false;
			if(!tokenBody.IsAuthorized(TenantId, writeAccess))
			{
				if (allowUnauthenticatedUsers)
					return true;

				WriteAuthorizationChallenge(ctx, 403, "insufficient_scope", 
					writeAccess ?
					"Not authorized for read/write access for tenant " + TenantId :
					"Not authorized for tenant " + TenantId);
	   
				return false;
			}
			
			ctx.User = new OAuthPrincipal(tokenBody, TenantId);
			CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = tokenBody.UserId;
			CurrentOperationContext.User.Value = ctx.User;
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
		private readonly string tenantId;

		public OAuthPrincipal(AccessTokenBody tokenBody, string tenantId)
		{
			this.tokenBody = tokenBody;
			this.tenantId = tenantId;
		}

		public bool IsInRole(string role)
		{
			if ("Administrators".Equals(role, StringComparison.InvariantCultureIgnoreCase) == false)
				return false;

			var databaseAccess = tokenBody.AuthorizedDatabases.FirstOrDefault(x=>string.Equals(x.TenantId, tenantId, StringComparison.InvariantCultureIgnoreCase) || x.TenantId == "*");

			if (databaseAccess == null)
				return false;

			return databaseAccess.Admin;
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