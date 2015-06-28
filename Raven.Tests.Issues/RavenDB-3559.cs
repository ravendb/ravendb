using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Issues
{
	public class RavenDB_3559 : RavenTestBase
	{
		private const string Operation = "Content/View";
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}
		[Fact]
		public void get_user_info()
		{
			using (var server = GetNewServer(enableAuthentication: true))
			{
				var serverUrl = ConfigureApiKeys(server);

				using (var store = new DocumentStore
				{
					Url = serverUrl,
					DefaultDatabase = "Foo",
					ApiKey = "Foo/ThisIsMySecret2"

				})
				{
					store.Initialize();
					using (var session = store.OpenSession())
					{
						var user = new User {Name = "Hila"};
						var contentWithoutPermission = new Content {Title = "Content Without Permission"};
						var contentWithPermission = new Content {Title = "Content With Permission"};
						session.Store(user);
						session.Store(contentWithoutPermission);
						session.Store(contentWithPermission);


						session.SaveChanges();

					}
					var info = store.DatabaseCommands.GetUserInfo();

					Assert.Equal(3, info.Databases.Count);

					Assert.Equal(3, info.AccessTokenBody.AuthorizedDatabases.Count);

					Assert.Equal("Foo" ,info.Databases[0].Database);
					Assert.Equal("db2", info.Databases[1].Database);
					Assert.Equal("db3", info.Databases[2].Database);

					Assert.True(info.Databases[0].IsAdmin);
					Assert.True(info.Databases[1].IsAdmin);
					Assert.False(info.Databases[2].IsAdmin);

					Assert.Equal(1, info.ReadOnlyDatabases.Count);


				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		private class Content
		{
			public string Id { get; set; }
			public string Title { get; set; }
		}

		private static string ConfigureApiKeys(RavenDbServer server)
		{
			server.SystemDatabase.Documents.Put("Raven/ApiKeys/sysadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "sysadmin",
				Secret = "ThisIsMySecret",
				Enabled = true,
				Databases = new List<ResourceAccess>
				{
					new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true},
				}
			}), new RavenJObject(), null);


			server.SystemDatabase.Documents.Put("Raven/ApiKeys/Foo", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "Foo",
				Secret = "ThisIsMySecret2",
				Enabled = true,
				Databases = new List<ResourceAccess>

				{
					new ResourceAccess {TenantId = "Foo", Admin = true},
					new ResourceAccess {TenantId = "db2", ReadOnly = true},
					new ResourceAccess {TenantId = "db3", Admin = false}
				}
			}), new RavenJObject(), null);

			var serverUrl = server.SystemDatabase.ServerUrl;

			using (var store = new DocumentStore()
			{
				Url = serverUrl,
				ApiKey = "sysadmin/ThisIsMySecret"
			})
			{
				store.Initialize();
				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Foo",
						Settings =
						{
							{"Raven/DataDir", "Foo"}
						}
					});
				store.DatabaseCommands.EnsureDatabaseExists("Foo");
			}
			return serverUrl;
		}
	}
}

