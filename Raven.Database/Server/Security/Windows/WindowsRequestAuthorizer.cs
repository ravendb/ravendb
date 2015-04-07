using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Threading;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.Controllers;

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
			var doc = server.SystemDatabase.Documents.Get("Raven/Authorization/WindowsSettings", null);

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

        public bool TryAuthorize(RavenBaseApiController controller, bool ignoreDb, out HttpResponseMessage msg)
		{
			Func<HttpResponseMessage> onRejectingRequest;
			var tenantId = controller.TenantName ?? Constants.SystemDatabase;
			var userCreated = TryCreateUser(controller, tenantId, out onRejectingRequest);
			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && userCreated == false)
			{
				msg = onRejectingRequest();
				return false;
			}

			PrincipalWithDatabaseAccess user = null;
			if (userCreated)
			{
				user = (PrincipalWithDatabaseAccess)controller.User;
				CurrentOperationContext.User.Value = controller.User;

				// admins always go through
				if (user.Principal.IsAdministrator(server.SystemConfiguration.AnonymousUserAccessMode))
				{
					msg = controller.GetEmptyMessage();
					return true;
				}

				// backup operators can go through
				if (user.Principal.IsBackupOperator(server.SystemConfiguration.AnonymousUserAccessMode))
				{
					msg = controller.GetEmptyMessage();
					return true;
				}
			}

            bool isGetRequest = IsGetRequest(controller);
			switch (server.SystemConfiguration.AnonymousUserAccessMode)
			{
				case AnonymousUserAccessMode.Admin:
				case AnonymousUserAccessMode.All:
					msg = controller.GetEmptyMessage();
					return true; // if we have, doesn't matter if we have / don't have the user
				case AnonymousUserAccessMode.Get:
					if (isGetRequest)
					{
						msg = controller.GetEmptyMessage();
						return true;
					}
					goto case AnonymousUserAccessMode.None;
				case AnonymousUserAccessMode.None:
					if (userCreated)
					{
						if (string.IsNullOrEmpty(tenantId) == false && tenantId.StartsWith("fs/"))
							tenantId = tenantId.Substring(3);

						if (string.IsNullOrEmpty(tenantId) == false && tenantId.StartsWith("counters/"))
							tenantId = tenantId.Substring(9);

					    if (user.AdminDatabases.Contains(tenantId) ||
					        user.AdminDatabases.Contains("*") || ignoreDb)
					    {
					        msg = controller.GetEmptyMessage();
					        return true;
					    }
					    if (user.ReadWriteDatabases.Contains(tenantId) ||
					        user.ReadWriteDatabases.Contains("*"))
					    {
					        msg = controller.GetEmptyMessage();
					        return true;
					    }
					    if (isGetRequest && (user.ReadOnlyDatabases.Contains(tenantId) ||
					                            user.ReadOnlyDatabases.Contains("*")))
					    {
					        msg = controller.GetEmptyMessage();
					        return true;
					    }
					}

					msg = onRejectingRequest();
					return false;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

        private bool TryCreateUser(RavenBaseApiController controller, string databaseName, out Func<HttpResponseMessage> onRejectingRequest)
		{
			var invalidUser = (controller.User == null || controller.User.Identity.IsAuthenticated == false);
			if (invalidUser)
			{
				onRejectingRequest = () =>
				{
					var msg = ProvideDebugAuthInfo(controller, new
					{
						Reason = "User is null or not authenticated"
					});
					controller.AddHeader("Raven-Required-Auth", "Windows", msg);
					if (string.IsNullOrEmpty(controller.SystemConfiguration.OAuthTokenServer) == false)
					{
						controller.AddHeader("OAuth-Source", controller.SystemConfiguration.OAuthTokenServer, msg);
					}
					msg.StatusCode = HttpStatusCode.Unauthorized;

					return msg;
				};
				return false;
			}

			var dbUsersIsAllowedAccessTo = requiredUsers
				.Where(data => controller.User.Identity.Name.Equals(data.Name, StringComparison.InvariantCultureIgnoreCase))
				.SelectMany(source => source.Databases)
				.Concat(requiredGroups.Where(windowsGroup => controller.User.IsInRole(windowsGroup.Name)).SelectMany(x => x.Databases))
				.ToList();

            var user = UpdateUserPrincipal(controller, dbUsersIsAllowedAccessTo);

			onRejectingRequest = () =>
			{
				var msg = ProvideDebugAuthInfo(controller, new
				{
					user.Identity.Name,
					user.AdminDatabases,
					user.ReadOnlyDatabases,
					user.ReadWriteDatabases,
					DatabaseName = databaseName
				});

				msg.StatusCode = HttpStatusCode.Forbidden;

				throw new HttpResponseException(msg);
			};
			return true;
		}

        private static HttpResponseMessage ProvideDebugAuthInfo(RavenBaseApiController controller, object msg)
		{
			string debugAuth = controller.GetQueryStringValue("debug-auth");
			if (debugAuth == null)
				return controller.GetEmptyMessage();

			bool shouldProvideDebugAuthInformation;
			if (bool.TryParse(debugAuth, out shouldProvideDebugAuthInformation) && shouldProvideDebugAuthInformation)
			{
				return controller.GetMessageWithObject(msg);
			}

			return controller.GetEmptyMessage();
		}

        private PrincipalWithDatabaseAccess UpdateUserPrincipal(RavenBaseApiController controller, List<ResourceAccess> databaseAccessLists)
        {
            var access = controller.User as PrincipalWithDatabaseAccess;
            if (access != null)
                return access;

            var user = new PrincipalWithDatabaseAccess((WindowsPrincipal)controller.User);

            foreach (var databaseAccess in databaseAccessLists)
            {
                if (databaseAccess.Admin)
                    user.AdminDatabases.Add(databaseAccess.TenantId);
                else if (databaseAccess.ReadOnly)
                    user.ReadOnlyDatabases.Add(databaseAccess.TenantId);
                else
                    user.ReadWriteDatabases.Add(databaseAccess.TenantId);
            }

            controller.User = user;
            Thread.CurrentPrincipal = user;

            return user;
        }

		public List<string> GetApprovedResources(IPrincipal user)
		{
			var winUser = user as PrincipalWithDatabaseAccess;
			if (winUser == null)
				return new List<string>();

			var list = new List<string>();
			list.AddRange(winUser.AdminDatabases);
			list.AddRange(winUser.ReadOnlyDatabases);
			list.AddRange(winUser.ReadWriteDatabases);

			return list;
		}

		public override void Dispose()
		{
			WindowsSettingsChanged -= UpdateSettings;
		}

		public IPrincipal GetUser(RavenDbApiController controller)
		{
			Func<HttpResponseMessage> onRejectingRequest;
			var databaseName = controller.DatabaseName ?? Constants.SystemDatabase;
			var userCreated = TryCreateUser(controller, databaseName, out onRejectingRequest);
			if (userCreated == false)
				onRejectingRequest();
			return userCreated ? controller.User : null;
		}
	}
}
