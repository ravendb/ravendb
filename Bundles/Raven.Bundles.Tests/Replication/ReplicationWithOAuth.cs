extern alias database;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
	public class ReplicationWithOAuth : ReplicationBase
	{
		protected override void ConfigureServer(database::Raven.Database.Config.RavenConfiguration serverConfiguration)
		{
			serverConfiguration.AnonymousUserAccessMode = database::Raven.Database.Server.AnonymousUserAccessMode.None;
			database::Raven.Database.Server.Security.Authentication.EnableOnce();
		}


		protected override void ConfigureStore(Client.Document.DocumentStore documentStore)
		{
			documentStore.ApiKey = "Ayende/abc";
			base.ConfigureStore(documentStore);
		}

		protected override void SetupDestination(Abstractions.Replication.ReplicationDestination replicationDestination)
		{
			replicationDestination.ApiKey = "Ayende/abc";
		}

		[Fact]
		public async Task CanReplicateDocumentWithOAuth()
		{
			var store1 = CreateStore(enableAuthorization:true);
			var store2 = CreateStore(enableAuthorization: true);

			foreach (var server in servers)
			{
				server.SystemDatabase.Put("Raven/ApiKeys/Ayende", null, RavenJObject.FromObject(new ApiKeyDefinition
					{
						Databases = new List<DatabaseAccess> { new DatabaseAccess { TenantId = "*" }, new DatabaseAccess { TenantId = "<system>" }, },
						Enabled = true,
						Name = "Ayende",
						Secret = "abc"
					}), new RavenJObject(), null);
			}

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Item());
				await session.SaveChangesAsync();
			}

			JsonDocument item = null;
			for (int i = 0; i < RetriesCount; i++)
			{
				item = await store2.AsyncDatabaseCommands.GetAsync("items/1");
				if (item != null)
					break;
				Thread.Sleep(100);
			}
			Assert.NotNull(item);
		}

		public class Item
		{
		}
	}
}