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
			var databaseName = database().Name ?? Constants.SystemDatabase;
			var userCreated = TryCreateUser(ctx, databaseName, out onRejectingRequest);
			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && userCreated == false)
			{
				onRejectingRequest();
				return false;
			}
			
			PrincipalWithDatabaseAccess user = null;
			if(userCreated)
			{
				user = (PrincipalWithDatabaseAccess)ctx.User;
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = ctx.User.Identity.Name;
				CurrentOperationContext.User.Value = ctx.User;
				
				// admins always go through
				if(user.Principal.IsAdministrator())
					return true;
			}
			

			var httpRequest = ctx.Request;
			bool isGetRequest = IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath);
			switch (server.SystemConfiguration.AnonymousUserAccessMode)
			{
				case AnonymousUserAccessMode.All:
					return true; // if we have, doesn't matter if we have / don't have the user
				case AnonymousUserAccessMode.Get:
					if (isGetRequest)
						return true;
					goto case AnonymousUserAccessMode.None;
				case AnonymousUserAccessMode.None:
					if (userCreated)
					{
						if (user.AdminDatabases.Contains(databaseName))
							return true;
						if (user.ReadWriteDatabases.Contains(databaseName))
							return true;
						if (isGetRequest && user.ReadOnlyDatabases.Contains(databaseName))
							return true;
					}

					onRejectingRequest();
					return false;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		private bool TryCreateUser(IHttpContext ctx, string databaseName, out Action onRejectingRequest)
		{
			var invalidUser = (ctx.User == null || ctx.User.Identity.IsAuthenticated == false);
			if (invalidUser)
			{
				onRejectingRequest = () =>
				{
					ProvideDebugAuthInfo(ctx, new
					{
						Reason = "User is null or not authenticated"
					});
					ctx.Response.AddHeader("Raven-Required-Auth", "Windows");
					if (string.IsNullOrEmpty(Settings.OAuthTokenServer) == false)
					{
						ctx.Response.AddHeader("OAuth-Source", Settings.OAuthTokenServer);
					}
					ctx.SetStatusToUnauthorized();
				};
				return false;
			}

			var databaseAccessLists = GenerateDatabaseAccessLists(ctx);
			var user = UpdateUserPrincipal(ctx, databaseAccessLists);

			onRejectingRequest = () =>
			{
				ctx.SetStatusToForbidden();

				ProvideDebugAuthInfo(ctx, new
				{
					user.ExplicitlyConfigured,
					user.Identity.Name,
					user.AdminDatabases,
					user.ReadOnlyDatabases,
					user.ReadWriteDatabases,
					DatabaseName = databaseName
				});
			};
			return true;
		}

		private static void ProvideDebugAuthInfo(IHttpContext ctx, object msg)
		{
			string debugAuth = ctx.Request.QueryString["debug-auth"];
			if(debugAuth  == null)
				return;

			bool shouldProvideDebugAuthInformation;
			if (bool.TryParse(debugAuth, out shouldProvideDebugAuthInformation) && shouldProvideDebugAuthInformation)
			{
				ctx.WriteJson(msg);
			}
		}

		private PrincipalWithDatabaseAccess UpdateUserPrincipal(IHttpContext ctx, Dictionary<string, List<DatabaseAccess>> databaseAccessLists)
		{
			if (ctx.User is PrincipalWithDatabaseAccess)
				return (PrincipalWithDatabaseAccess) ctx.User;

			var user = new PrincipalWithDatabaseAccess((WindowsPrincipal) ctx.User);
			
			List<DatabaseAccess> list;
			if (databaseAccessLists.TryGetValue(ctx.User.Identity.Name, out list) == false)
			{
				ctx.User = user;
				user.ExplicitlyConfigured = false;
				return user;
			}
			user.ExplicitlyConfigured = true;

			foreach (var databaseAccess in list) 
			{
				if (databaseAccess.Admin)
					user.AdminDatabases.Add(databaseAccess.TenantId);
				else if (databaseAccess.ReadOnly)
					user.ReadOnlyDatabases.Add(databaseAccess.TenantId);
				else
					user.ReadWriteDatabases.Add(databaseAccess.TenantId);
			}

			ctx.User = user;
			return user;
		}

		private Dictionary<string, List<DatabaseAccess>> GenerateDatabaseAccessLists(IHttpContext ctx)
		{
			var databaseAccessLists = requiredUsers
				.Where(data => ctx.User.Identity.Name.Equals(data.Name, StringComparison.InvariantCultureIgnoreCase))
				.ToDictionary(source => source.Name, source => source.Databases, StringComparer.InvariantCultureIgnoreCase);

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