using System;
using System.Collections.Generic;
using System.Security.Principal;
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
			UpdateSettings();
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
			//TODO: Check that the User is valid with the PrincapalWithDatabaseAccress and check if administrator
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
			//TODO: kiil this method and change it to TryCreateUser
			var invalidUser = (ctx.User == null || ctx.User.Identity.IsAuthenticated == false);
			if (invalidUser)
			{
				onRejectingRequest = () =>
				{
					ctx.Response.AddHeader("Raven-Required-Auth", "Windows");
					ctx.SetStatusToForbidden();
				};
				return true;
			}

			onRejectingRequest = ctx.SetStatusToUnauthorized;

			List<DatabaseAccess> databasesForGroups;
			var databasesForUsers = GenerateDatabaseAccessLists(ctx, out databasesForGroups);

			var adminList = GenerateAdminList(databasesForUsers, databasesForGroups);

			if (ctx.User is PrincipalWithDatabaseAccess == false)
				ctx.User = new PrincipalWithDatabaseAccess((WindowsPrincipal)ctx.User, adminList);

			var readOnlyList = GenerateReadOnlyList(databasesForUsers, databasesForGroups);

			if ((requiredGroups.Count > 0 || requiredUsers.Count > 0))
			{
				var databaseName = database().Name;

				if (readOnlyList.Any(selectedDatabaseName => string.Equals(selectedDatabaseName, databaseName)))
					return true;
				if (requiredGroups.Any(requiredGroup => ctx.User.IsInRole(requiredGroup.Name)
					&& requiredGroup.Databases.Any(access => access.TenantId == databaseName))
					|| requiredUsers.Any(requiredUser => string.Equals(ctx.User.Identity.Name, requiredUser.Name, StringComparison.InvariantCultureIgnoreCase)
						&& requiredUser.Databases.Any(access => access.TenantId == databaseName)))
					return false;

				return true;
			}

			return false;
		}

		private static IEnumerable<string> GenerateReadOnlyList(IEnumerable<DatabaseAccess> databasesForUsers, IEnumerable<DatabaseAccess> databasesForGroups)
		{
			var readOnlyList = new List<string>();
			if (databasesForUsers != null)
				readOnlyList = databasesForUsers
					.Where(access => access.ReadOnly)
					.Select(access => access.TenantId)
					.ToList();

			if (databasesForGroups != null)
				readOnlyList
					.AddRange(databasesForGroups.Where(access => access.ReadOnly)
								  .Select(access => access.TenantId)
								  .ToList());
			return readOnlyList;
		}

		private static List<string> GenerateAdminList(IEnumerable<DatabaseAccess> databasesForUsers, IEnumerable<DatabaseAccess> databasesForGroups)
		{
			var adminList = new List<string>();
			if (databasesForUsers != null)
				adminList = databasesForUsers
					.Where(access => access.Admin)
					.Select(access => access.TenantId)
					.ToList();


			if (databasesForGroups != null)
				adminList
					.AddRange(databasesForGroups.Where(access => access.Admin)
								  .Select(access => access.TenantId)
								  .ToList());
			return adminList;
		}

		private List<DatabaseAccess> GenerateDatabaseAccessLists(IHttpContext ctx, out List<DatabaseAccess> databasesForGroups)
		{
			var databasesForUsers = requiredUsers
				.Where(data => ctx.User.Identity.Name.Equals(data.Name, StringComparison.InvariantCultureIgnoreCase))
				.Select(data => data.Databases)
				.FirstOrDefault();

			databasesForGroups = requiredGroups
				.Where(data => ctx.User.IsInRole(data.Name))
				.Select(data => data.Databases)
				.FirstOrDefault();

			return databasesForUsers;
		}

		public override void Dispose()
		{
			WindowsSettingsChanged -= UpdateSettings;
		}
	}
}