using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Security.OAuth
{
	public class ReplicateWithOAuth : ReplicationBase
	{
		private const string apiKey = "test/ThisIsMySecret";

		protected override void ConfigureStore(DocumentStore store)
		{
			store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
			store.Credentials = null;
			store.ApiKey = apiKey;
		}

		protected override void ConfigureDatabase(Database.DocumentDatabase database)
		{
			database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "test",
				Secret = "ThisIsMySecret",
				Enabled = true,
				Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess {TenantId = "*"},
					new DatabaseAccess {TenantId = Constants.SystemDatabase},
				}
			}), new RavenJObject(), null);
		}

		[Fact]
		public void Can_Replicate_With_OAuth()
		{
			var store1 = CreateStore(enableAuthorization: true);
            Authentication.EnableOnce();
            var store2 = CreateStore(anonymousUserAccessMode: AnonymousUserAccessMode.None, enableAuthorization: true);
			
			TellFirstInstanceToReplicateToSecondInstance(apiKey);

			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			var company = WaitForDocument<Company>(store2, "companies/1");
			Assert.Equal("Hibernating Rhinos", company.Name);
		}
	}
}
