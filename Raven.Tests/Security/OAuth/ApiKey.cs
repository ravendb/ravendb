using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Server;
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

		protected override void ModifyStore(Client.Document.DocumentStore store)
		{
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
