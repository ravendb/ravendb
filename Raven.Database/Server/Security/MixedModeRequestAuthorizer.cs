using System;
using System.Collections.Generic;
using System.Security.Principal;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security.OAuth;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server.Security
{
	public class MixedModeRequestAuthorizer : AbstractRequestAuthorizer
	{
		private readonly WindowsRequestAuthorizer windowsRequestAuthorizer = new WindowsRequestAuthorizer();
		private readonly OAuthRequestAuthorizer oAuthRequestAuthorizer = new OAuthRequestAuthorizer();

		protected override void Initialize()
		{
			windowsRequestAuthorizer.Initialize(database, settings, tenantId, server);
			oAuthRequestAuthorizer.Initialize(database, settings, tenantId, server);
			base.Initialize();
		}

		public bool Authorize(IHttpContext context)
		{
			var requestUrl = context.GetRequestUrl();
			if ( NeverSecret.Urls.Contains(requestUrl))
				return true;

			var hasApiKey = "True".Equals(context.Request.Headers["Has-Api-Key"], StringComparison.CurrentCultureIgnoreCase);
			var authHeader = context.Request.Headers["Authorization"];
			var hasOAuthTokenInCookie = context.Request.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie || 
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.Authorize(context, hasApiKey);
			}

			return windowsRequestAuthorizer.Authorize(context);
		}

		public IPrincipal GetUser(IHttpContext context)
		{
			var hasApiKey = "True".Equals(context.Request.Headers["Has-Api-Key"], StringComparison.CurrentCultureIgnoreCase);
			var authHeader = context.Request.Headers["Authorization"];
			var hasOAuthTokenInCookie = context.Request.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie ||
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetUser(context, hasApiKey);
			}

			return windowsRequestAuthorizer.GetUser(context);
		}

		public override List<string> GetApprovedDatabases(IPrincipal user, IHttpContext context)
		{
			var authHeader = context.Request.Headers["Authorization"];
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetApprovedDatabases(user);
			}

			return windowsRequestAuthorizer.GetApprovedDatabases(user);
		}

		public override void Dispose()
		{
			windowsRequestAuthorizer.Dispose();
			oAuthRequestAuthorizer.Dispose();
		}
	}
}