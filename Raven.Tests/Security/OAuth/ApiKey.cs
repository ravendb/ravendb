using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Security.OAuth
{
	public class ApiKey : RemoteClientTest
	{
		private const string apiKey = "test/ThisIsMySecret";

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
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

			using (var store = NewRemoteDocumentStore())
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

		class TestClass
		{
			public string Name { get; set; }
			public string Id { get; set; }
		}
	}
}
