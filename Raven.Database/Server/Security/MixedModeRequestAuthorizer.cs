using System;
using System.Collections.Generic;
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

		public override bool Authorize(IHttpContext context)
		{
			var requestUrl = context.GetRequestUrl();
			if (NeverSecret.Urls.Contains(requestUrl))
				return true;

			var authHeader = context.Request.Headers["Authorization"];
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.Authorize(context);
			}

			return windowsRequestAuthorizer.Authorize(context);
		}

		public override List<string> GetApprovedDatabases(IHttpContext context)
		{
			var authHeader = context.Request.Headers["Authorization"];
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetApprovedDatabases(context);
			}

			return windowsRequestAuthorizer.GetApprovedDatabases(context);
		}

		public override void Dispose()
		{
			windowsRequestAuthorizer.Dispose();
			oAuthRequestAuthorizer.Dispose();
		}
	}
}