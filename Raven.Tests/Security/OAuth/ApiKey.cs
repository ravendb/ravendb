using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Security.OAuth
{
	public class ApiKey : RemoteClientTest
	{
		private const string apiKey = "test/ThisIsMySecret";

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;


            Authentication.EnableOnce();
        }

		protected override void ModifyServer(Server.RavenDbServer ravenDbServer)
		{
			ravenDbServer.Database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "test",
				Secret = "ThisIsMySecret",
				Enabled = true,
				Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess{TenantId = "*"}, 
					new DatabaseAccess{TenantId = Constants.SystemDatabase}, 
				}
			}), new RavenJObject(), null);
		}

		protected override void ModifyStore(DocumentStore store)
		{
			store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
			store.Credentials = null;
			store.ApiKey = apiKey;
		}

		[Fact]
		public void TestApiKeyStoreAndLoad()
		{
			const string id = "test/1";
			const string name = "My name";

			using (var store = NewRemoteDocumentStore(enableAuthentication: true))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestClass { Name = name, Id = id });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(name, session.Load<TestClass>(id).Name);
				}
			}
		}

		[Fact]
		public void CanAuthAsAdminAgainstTenantDb()
		{
			using (var server = GetNewServer(enableAuthentication: true))
			{

				server.Database.Put("Raven/ApiKeys/sysadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "sysadmin",
					Secret = "ThisIsMySecret",
					Enabled = true,
					Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess{TenantId = Constants.SystemDatabase, Admin = true}, 
				}
				}), new RavenJObject(), null);

				server.Database.Put("Raven/ApiKeys/dbadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "dbadmin",
					Secret = "ThisIsMySecret",
					Enabled = true,
					Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess{TenantId = "*", Admin = true}, 
					new DatabaseAccess{TenantId = Constants.SystemDatabase, Admin = false}, 
				}
				}), new RavenJObject(), null);

				using (var store = new DocumentStore
				{
					Url = server.Database.ServerUrl,
					ApiKey = "sysadmin/ThisIsMySecret",
					Conventions = {FailoverBehavior = FailoverBehavior.FailImmediately}
				}.Initialize())
				{
					store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("test");
				}

				using (var store = new DocumentStore
				{
					Url = server.Database.ServerUrl,
					ApiKey = "dbadmin/ThisIsMySecret"
				}.Initialize())
				{
					store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url + "/databases/test/admin/changeDbId", "POST", null, store.Conventions))
						.ExecuteRequest();// can do admin stuff

					var httpJsonRequest = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url + "/databases/test/debug/user-info", "GET", null, store.Conventions));

					var json = (RavenJObject)httpJsonRequest.ReadResponseJson();

					Assert.True(json.Value<bool>("IsAdminCurrentDb"));
				}
			}
		}

		class TestClass
		{
			public string Name { get; set; }
			public string Id { get; set; }
		}
	}
}
