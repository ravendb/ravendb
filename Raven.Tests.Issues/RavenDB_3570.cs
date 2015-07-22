using System;
using System.Collections.Generic;
using System.DirectoryServices.AccountManagement;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.FileSystem;
using Raven.Database.Server;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{

	public class RavenDB_3570 : RavenFilesTestBase
	{
		private const string username = "local_user_test";

		private const string password = "local_user_test";


		protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
		{
						server.SystemDatabase.Documents.Put("Raven/Authorization/WindowsSettings", null,
												  RavenJObject.FromObject(new WindowsAuthDocument
												  {
													  RequiredUsers = new List<WindowsAuthData>
			                                          {
			                                              new WindowsAuthData
			                                              {
			                                                  Name = string.Format("{0}\\{1}", null, username),
			                                                  Enabled = true,
			                                                  Databases = new List<ResourceAccess>
			                                                  {
			                                                      new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
																  new ResourceAccess {TenantId = fileSystemName}
			                                                  }
			                                              }
			                                          }
												  }), new RavenJObject(), null);
		}

		//requires admin context
		[Fact]
		public async Task RavenFSWithWindowsCredentialsInConnectionStringShouldWork()
		{
			try
			{
				AddWindowsUser(username, password);

				using (var filesStore = NewStore(enableAuthentication: true))
				{
					var ms = new MemoryStream(new byte[] { 1, 2, 4 });
					await filesStore.AsyncFilesCommands.UploadAsync("/dir/ms.bin", ms);
				}
			}
			finally
			{
				DeleteUser(username);
			}
		}

		private void DeleteUser(string username)
		{
			using (var context = new PrincipalContext(ContextType.Machine))
			using (var up = UserPrincipal.FindByIdentity(context, username))
			{
				if (up != null)
					up.Delete();
			}
		}

		private void AddWindowsUser(string username, string password, string displayName = null, string description = null, bool canChangePassword = true, bool passwordExpires = false)
		{
			using (var context = new PrincipalContext(ContextType.Machine))
			using (var user = new UserPrincipal(context, username, password, true))
			using (var up = UserPrincipal.FindByIdentity(context, username))
			{
				if (up != null)
					up.Delete();

				user.UserCannotChangePassword = !canChangePassword;
				user.PasswordNeverExpires = !passwordExpires;
				user.Description = description ?? String.Empty;
				user.DisplayName = displayName ?? username;
				user.Save();
			}
		}

	}
}
