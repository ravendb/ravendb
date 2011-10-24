using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsRequestAuthorizer : AbstractRequestAuthorizer
	{
		readonly string[] neverSecretUrls = new[]
			{
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
			var requestUrl = ctx.GetRequestUrl();

			if (neverSecretUrls.Contains(requestUrl, StringComparer.InvariantCultureIgnoreCase))
				return true;


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
			return (ctx.User == null || 
				ctx.User.Identity == null || 
				ctx.User.Identity.IsAuthenticated == false) || 
				ctx.User.IsInRole();
		}
	}
}