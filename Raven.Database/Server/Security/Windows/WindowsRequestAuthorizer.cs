using System;
using System.Collections.Generic;
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

		private readonly List<string> requiredGroups = new List<string>();

		protected override void Initialize()
		{
			var requiredGroupsString = server.Configuration.Settings["Raven/Authorization/Windows/RequiredGroups"];
			if (requiredGroupsString == null)
				return;

			var groups = requiredGroupsString.Split(new[]{';'}, StringSplitOptions.RemoveEmptyEntries);
			requiredGroups.AddRange(groups);
		}

		public override bool Authorize(IHttpContext ctx)
		{
			if (server.DefaultConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && IsInvalidUser(ctx))
			{
				var requestUrl = ctx.GetRequestUrl();
				if (neverSecretUrls.Contains(requestUrl, StringComparer.InvariantCultureIgnoreCase))
					return true;

				ctx.SetStatusToUnauthorized();
				return false;
			}

			var httpRequest = ctx.Request;

			if (server.DefaultConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				IsInvalidUser(ctx) &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
				var requestUrl = ctx.GetRequestUrl();
				if (neverSecretUrls.Contains(requestUrl, StringComparer.InvariantCultureIgnoreCase))
					return true;

				ctx.SetStatusToUnauthorized();
				return false;
			}

			return true;
		}

	  

		private bool IsInvalidUser(IHttpContext ctx)
		{
			var invalidUser = (ctx.User == null || 
			                     ctx.User.Identity == null || 
			                     ctx.User.Identity.IsAuthenticated == false);
			if(invalidUser == false &&  requiredGroups.Count > 0)
			{
				return requiredGroups.All(requiredGroup => !ctx.User.IsInRole(requiredGroup));
			}
			return invalidUser;
		}
	}
}