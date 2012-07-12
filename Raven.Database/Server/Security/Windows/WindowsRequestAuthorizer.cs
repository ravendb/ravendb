using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsRequestAuthorizer : AbstractRequestAuthorizer
	{
		private readonly List<string> requiredGroups = new List<string>();
		private readonly List<string> requiredUsers = new List<string>();

		protected override void Initialize()
		{
			var requiredGroupsString = server.Configuration.Settings["Raven/Authorization/Windows/RequiredGroups"];
			if (requiredGroupsString != null)
			{
				var groups = requiredGroupsString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				requiredGroups.AddRange(groups);
			}

			var requiredUsersString = server.Configuration.Settings["Raven/Authorization/Windows/RequiredUsers"];
			if (requiredUsersString != null)
			{
				var users = requiredUsersString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				requiredUsers.AddRange(users);
			}
		}

		public override bool Authorize(IHttpContext ctx)
		{
			Action onRejectingRequest;
			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && IsInvalidUser(ctx, out onRejectingRequest))
			{
				var requestUrl = ctx.GetRequestUrl();
				if (NeverSecret.Urls.Contains(requestUrl))
					return true;

				onRejectingRequest();
				return false;
			}

			var httpRequest = ctx.Request;

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				IsInvalidUser(ctx, out onRejectingRequest) &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
				var requestUrl = ctx.GetRequestUrl();
				if (NeverSecret.Urls.Contains(requestUrl))
					return true;

				onRejectingRequest();
				return false;
			}

			if (IsInvalidUser(ctx, out onRejectingRequest) == false)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = ctx.User.Identity.Name;
				CurrentOperationContext.User.Value = ctx.User;
			}
			return true;
		}

		private bool IsInvalidUser(IHttpContext ctx, out Action onRejectingRequest)
		{
			var invalidUser = (ctx.User == null ||
							   ctx.User.Identity.IsAuthenticated == false);
			if (invalidUser)
			{
				onRejectingRequest = () => ctx.SetStatusToForbidden();
				return true;
			}


			onRejectingRequest = () => ctx.SetStatusToUnauthorized();

			if (requiredGroups.Count > 0 || requiredUsers.Count > 0)
			{
			
				if (requiredGroups.Any(requiredGroup => ctx.User.IsInRole(requiredGroup)) ||
					requiredUsers.Any(requiredUser => string.Equals(ctx.User.Identity.Name, requiredUser, StringComparison.InvariantCultureIgnoreCase)))
					return false;

				return true;
			}
			
			return false;
		}
	}
}