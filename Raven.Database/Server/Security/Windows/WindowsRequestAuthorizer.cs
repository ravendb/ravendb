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
			Action onRejectingRequest;
			var userCreated = TryCreateUser(ctx, out onRejectingRequest);
			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && userCreated == false)
			{
				onRejectingRequest();
				return false;
			}
			PrincipalWithDatabaseAccess user = null;
			if(userCreated)
			{
				user = (PrincipalWithDatabaseAccess)ctx.User;
			}

			var databaseName = database().Name ?? Constants.SystemDatabase;

			if (userCreated && (user.Principal.IsAdministrator() || user.AdminDatabases.Contains(databaseName)))
				return true;

			var httpRequest = ctx.Request;

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				userCreated &&
				user.ReadWriteDatabases.Contains(databaseName) == false &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
				onRejectingRequest();
				return false;
			}

			if (IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) &&
				userCreated &&
				(user.ReadOnlyDatabases.Contains(databaseName) || user.ReadWriteDatabases.Contains(databaseName)))
				return true;

			if (userCreated)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = ctx.User.Identity.Name;
				CurrentOperationContext.User.Value = ctx.User;
			}

			return true;
		}

		private bool TryCreateUser(IHttpContext ctx, out Action onRejectingRequest)
		{
			var invalidUser = (ctx.User == null || ctx.User.Identity.IsAuthenticated == false);
			if (invalidUser)
			{
				onRejectingRequest = () =>
				{
					ctx.Response.AddHeader("Raven-Required-Auth", "Windows");
					ctx.SetStatusToForbidden();
				};
				return false;
			}

			var databaseAccessLists = GenerateDatabaseAccessLists(ctx);
			UpdateUserPrincipal(ctx, databaseAccessLists);

			onRejectingRequest = ctx.SetStatusToUnauthorized;
			return true;
		}

		private void UpdateUserPrincipal(IHttpContext ctx, Dictionary<string, List<DatabaseAccess>> databaseAccessLists)
		{
			if (ctx.User is PrincipalWithDatabaseAccess)
				return;

			var adminList = new List<string>();
			var readOnlyList = new List<string>();
			var readWriteList = new List<string>();

			if (databaseAccessLists.ContainsKey(ctx.User.Identity.Name) == false)
			{
				ctx.User = new PrincipalWithDatabaseAccess((WindowsPrincipal)ctx.User);
				return;
			}

			foreach (var databaseAccess in databaseAccessLists[ctx.User.Identity.Name])
			{
				if (databaseAccess.Admin)
					adminList.Add(databaseAccess.TenantId);
				else if (databaseAccess.ReadOnly)
					readOnlyList.Add(databaseAccess.TenantId);
				else
					readWriteList.Add(databaseAccess.TenantId);
			}

			ctx.User = new PrincipalWithDatabaseAccess((WindowsPrincipal)ctx.User)
			{
				AdminDatabases = adminList,
				ReadOnlyDatabases = readOnlyList,
				ReadWriteDatabases = readWriteList
			};
		}

		private Dictionary<string, List<DatabaseAccess>> GenerateDatabaseAccessLists(IHttpContext ctx)
		{
			var databaseAccessLists = requiredUsers
				.Where(data => ctx.User.Identity.Name.Equals(data.Name, StringComparison.InvariantCultureIgnoreCase))
				.ToDictionary(source => source.Name, source => source.Databases);

			foreach (var windowsAuthData in requiredGroups.Where(data => ctx.User.IsInRole(data.Name)))
			{
				if (databaseAccessLists.ContainsKey(windowsAuthData.Name))
				{
					databaseAccessLists[windowsAuthData.Name].AddRange(windowsAuthData.Databases);
				}
				else
				{
					databaseAccessLists.Add(windowsAuthData.Name, windowsAuthData.Databases);
				}
			}

			return databaseAccessLists;
		}

		public override List<string> GetApprovedDatabases(IHttpContext context)
		{
			var user = context.User as PrincipalWithDatabaseAccess;
			if(user == null)
				return new List<string>();

			var list = new List<string>();
			list.AddRange(user.AdminDatabases);
			list.AddRange(user.ReadOnlyDatabases);
			list.AddRange(user.ReadWriteDatabases);

			return list;
		}

		public override void Dispose()
		{
			WindowsSettingsChanged -= UpdateSettings;
		}
	}
}