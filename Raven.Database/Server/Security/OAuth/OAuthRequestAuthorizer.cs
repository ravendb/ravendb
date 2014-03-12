using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security.OAuth;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthRequestAuthorizer : AbstractRequestAuthorizer
	{
        public bool TryAuthorize(RavenBaseApiController controller, bool hasApiKey, bool ignoreDbAccess, out HttpResponseMessage msg)
		{
			var isGetRequest = IsGetRequest(controller.InnerRequest.Method.Method, controller.InnerRequest.RequestUri.AbsolutePath);
			var allowUnauthenticatedUsers = // we need to auth even if we don't have to, for bundles that want the user 
				Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.All ||
				Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin ||
					Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
					isGetRequest;

			var token = GetToken(controller);

			if (token == null)
			{
				if (allowUnauthenticatedUsers)
				{
					msg = controller.GetEmptyMessage();
					return true;
				}

				msg = WriteAuthorizationChallenge(controller, hasApiKey ? 412 : 401, "invalid_request", "The access token is required");

				return false;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(Settings.OAuthTokenKey, token, out tokenBody))
			{
				if (allowUnauthenticatedUsers)
				{
					msg = controller.GetEmptyMessage();
					return true;
				}

				msg = WriteAuthorizationChallenge(controller, 401, "invalid_token", "The access token is invalid");

				return false;
			}

			if (tokenBody.IsExpired())
			{
				if (allowUnauthenticatedUsers)
				{
					msg = controller.GetEmptyMessage();
					return true;
				}

				msg = WriteAuthorizationChallenge(controller, 401, "invalid_token", "The access token is expired");

				return false;
			}

			var writeAccess = isGetRequest == false;
            if (!tokenBody.IsAuthorized(controller.TenantName, writeAccess))
			{
				if (allowUnauthenticatedUsers || ignoreDbAccess)
				{
					msg = controller.GetEmptyMessage();
					return true;
				}

				msg = WriteAuthorizationChallenge(controller, 403, "insufficient_scope",
					writeAccess ?
                    "Not authorized for read/write access for tenant " + controller.TenantName :
					"Not authorized for tenant " + controller.TenantName);

				return false;
			}

            controller.User = new OAuthPrincipal(tokenBody, controller.TenantName);
			CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = tokenBody.UserId;
			CurrentOperationContext.User.Value = controller.User;
			msg = controller.GetEmptyMessage();

			return true;
		}

		public List<string> GetApprovedDatabases(IPrincipal user)
		{
			var oAuthUser = user as OAuthPrincipal;
			if (oAuthUser == null)
				return new List<string>();
			return oAuthUser.GetApprovedDatabases();
		}

        public List<string> GetApprovedFileSystems(IPrincipal user)
        {
            var oAuthUser = user as OAuthPrincipal;
            if (oAuthUser == null)
                return new List<string>();
            return oAuthUser.GetApprovedFileSystems();
        }

		public override void Dispose()
		{

		}

		static string GetToken(IHttpContext ctx)
		{
			const string bearerPrefix = "Bearer ";

			var auth = ctx.Request.Headers["Authorization"];
			if (auth == null)
			{
				auth = ctx.Request.GetCookie("OAuth-Token");
				if (auth != null)
					auth = Uri.UnescapeDataString(auth);
			}
			if (auth == null || auth.Length <= bearerPrefix.Length ||
				!auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
				return null;

			var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);

			return token;
		}

        static string GetToken(RavenBaseApiController controller)
		{
			const string bearerPrefix = "Bearer ";

			var auth = controller.GetHeader("Authorization");
			if (auth == null)
			{
				auth = controller.GetCookie("OAuth-Token");
				if (auth != null)
					auth = Uri.UnescapeDataString(auth);
			}
			if (auth == null || auth.Length <= bearerPrefix.Length ||
				!auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
				return null;

			var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);

			return token;
		}

		void WriteAuthorizationChallenge(IHttpContext ctx, int statusCode, string error, string errorDescription)
		{
			if (string.IsNullOrEmpty(Settings.OAuthTokenServer) == false)
			{
				if (Settings.UseDefaultOAuthTokenServer == false)
				{
					ctx.Response.AddHeader("OAuth-Source", Settings.OAuthTokenServer);
				}
				else
				{
					ctx.Response.AddHeader("OAuth-Source", new UriBuilder(Settings.OAuthTokenServer)
					{
						Host = ctx.Request.Url.Host,
						Port = ctx.Request.Url.Port
					}.Uri.ToString());

				}
			}
			ctx.Response.StatusCode = statusCode;
			ctx.Response.AddHeader("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription));
		}

        HttpResponseMessage WriteAuthorizationChallenge(RavenBaseApiController controller, int statusCode, string error, string errorDescription)
		{
			var msg = controller.GetEmptyMessage();
			var systemConfiguration = controller.SystemConfiguration;
			if (string.IsNullOrEmpty(systemConfiguration.OAuthTokenServer) == false)
			{
				if (systemConfiguration.UseDefaultOAuthTokenServer == false)
				{
					controller.AddHeader("OAuth-Source", systemConfiguration.OAuthTokenServer, msg);
				}
				else
				{
					controller.AddHeader("OAuth-Source", new UriBuilder(systemConfiguration.OAuthTokenServer)
					{
						Host = controller.InnerRequest.RequestUri.Host,
						Port = controller.InnerRequest.RequestUri.Port
					}.Uri.ToString(), msg);

				}
			}
			msg.StatusCode = (HttpStatusCode)statusCode;
 
			msg.Headers.Add("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription));

			return msg;
		}

		public IPrincipal GetUser(IHttpContext ctx, bool hasApiKey)
		{
			var token = GetToken(ctx);

			if (token == null)
			{
				WriteAuthorizationChallenge(ctx, hasApiKey ? 412 : 401, "invalid_request", "The access token is required");

				return null;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(Settings.OAuthTokenKey, token, out tokenBody))
			{
				WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is invalid");

				return null;
			}

			return new OAuthPrincipal(tokenBody, null);
		}

		public IPrincipal GetUser(RavenDbApiController controller, bool hasApiKey)
		{
			var token = GetToken(controller);

			if (token == null)
			{
				WriteAuthorizationChallenge(controller, hasApiKey ? 412 : 401, "invalid_request", "The access token is required");

				return null;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(controller.DatabasesLandlord.SystemConfiguration.OAuthTokenKey, token, out tokenBody))
			{
				WriteAuthorizationChallenge(controller, 401, "invalid_token", "The access token is invalid");

				return null;
			}

			return new OAuthPrincipal(tokenBody, null);
		}
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
		if ("Administrators".Equals(role, StringComparison.OrdinalIgnoreCase) == false)
			return false;

		var databaseAccess = tokenBody.AuthorizedDatabases
			.Where(x =>
				string.Equals(x.TenantId, tenantId, StringComparison.OrdinalIgnoreCase) ||
				x.TenantId == "*");

		return databaseAccess.Any(access => access.Admin);
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

	public List<string> GetApprovedDatabases()
	{
		return tokenBody.AuthorizedDatabases.Select(access => access.TenantId).ToList();
	}

    public List<string> GetApprovedFileSystems()
    {
        return tokenBody.AuthorizedFileSystems.Select(access => access.TenantId).ToList();
    }

	public AccessTokenBody TokenBody
	{
		get { return tokenBody; }
	}
}