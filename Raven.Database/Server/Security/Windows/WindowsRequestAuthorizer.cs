using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsRequestAuthorizer : AbstractRequestAuthorizer
	{
		private List<WindowsAuthData> requiredGroups = new List<WindowsAuthData>();
		private List<WindowsAuthData> requiredUsers = new List<WindowsAuthData>();

		private static event Action WindowsSettingsChanged = delegate { };

		public static void InvokeWindowsSettingsChanged()
		{
			WindowsSettingsChanged();
		}

		protected override void Initialize()
		{
			WindowsSettingsChanged += UpdateSettings;
		}

		public void UpdateSettings()
		{
			var doc = server.SystemDatabase.Get("Raven/Authorization/WindowsSettings", null);

			if (doc == null)
			{
				requiredGroups = new List<WindowsAuthData>();
				requiredUsers = new List<WindowsAuthData>();
				return;
			}

			var required = doc.DataAsJson.JsonDeserialization<WindowsAuthDocument>();
			if (required == null)
			{
				requiredGroups = new List<WindowsAuthData>();
				requiredUsers = new List<WindowsAuthData>();
				return;
			}

			requiredGroups = required.RequiredGroups != null
				                 ? required.RequiredGroups.Where(data => data.Enabled).ToList()
				                 : new List<WindowsAuthData>();
			requiredUsers = required.RequiredUsers != null
				                ? required.RequiredUsers.Where(data => data.Enabled).ToList()
				                : new List<WindowsAuthData>();
		}

		public override bool Authorize(IHttpContext ctx)
		{
			Action onRejectingRequest;
			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && IsInvalidUser(ctx, out onRejectingRequest))
			{
				onRejectingRequest();
				return false;
			}

			var httpRequest = ctx.Request;

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				IsInvalidUser(ctx, out onRejectingRequest) &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
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
				onRejectingRequest = ctx.SetStatusToForbidden;
				return true;
			}

			onRejectingRequest = ctx.SetStatusToUnauthorized;

			if (requiredGroups.Count > 0 || requiredUsers.Count > 0)
			{
				if (requiredGroups.Any(requiredGroup => ctx.User.IsInRole(requiredGroup.Name) && requiredGroup.Databases.Any(access => access.TenantId == database().Name)) ||
					requiredUsers.Any(requiredUser => string.Equals(ctx.User.Identity.Name, requiredUser.Name, StringComparison.InvariantCultureIgnoreCase) && requiredUser.Databases.Any(access => access.TenantId == database().Name)))
					return false;

				return true;
			}

			return false;
		}

		public override void Dispose()
		{
			WindowsSettingsChanged -= UpdateSettings;
		}
	}
}