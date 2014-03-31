using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class ReplicationWithOAuth : ReplicationBase
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration serverConfiguration)
		{
			serverConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}


		protected override void ModifyStore(DocumentStore documentStore)
		{
			documentStore.ApiKey = "Ayende/abc";
		}

		protected override void SetupDestination(Raven.Abstractions.Replication.ReplicationDestination replicationDestination)
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
				server.SystemDatabase.Documents.Put("Raven/ApiKeys/Ayende", null, RavenJObject.FromObject(new ApiKeyDefinition
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