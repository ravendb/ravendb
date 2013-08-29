using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Async
{
	public class ReadStriping : ReplicationBase
	{
		[Fact]
		public async Task When_replicating_can_do_read_striping()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			WaitForDocument(store2.DatabaseCommands, "companies/1");
			WaitForDocument(store3.DatabaseCommands, "companies/1");

			using(var store = new DocumentStore
			{
				Url = store1.Url,
				Conventions =
					{
						FailoverBehavior = FailoverBehavior.ReadFromAllServers
					}
			})
			{
				store.Initialize();
				var replicationInformerForDatabase = store.GetReplicationInformerForDatabase();
				await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient) store.DatabaseCommands);
				Assert.Equal(2, replicationInformerForDatabase.ReplicationDestinationsUrls.Count);

				foreach (var ravenDbServer in servers)
				{
					ravenDbServer.Server.ResetNumberOfRequests();
				}

				for (int i = 0; i < 6; i++)
				{
					using(var session = store.OpenAsyncSession())
					{
						Assert.NotNull(await session.LoadAsync<Company>("companies/1"));
					}
				}
			}
			foreach (var ravenDbServer in servers)
			{
				Assert.Equal(2, ravenDbServer.Server.NumberOfRequests);
			}
		}

		[Fact]
		public async Task Can_avoid_read_striping()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company());
				await session.SaveChangesAsync();
			}

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			WaitForDocument(store2.DatabaseCommands, "companies/1");
			WaitForDocument(store3.DatabaseCommands, "companies/1");

			using (var store = new DocumentStore
			{
				Url = store1.Url,
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.ReadFromAllServers
				}
			})
			{
				store.Initialize();
				var replicationInformerForDatabase = store.GetReplicationInformerForDatabase();
				await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient)store.DatabaseCommands);
				Assert.Equal(2, replicationInformerForDatabase.ReplicationDestinations.Count);

				foreach (var ravenDbServer in servers)
				{
					ravenDbServer.Server.ResetNumberOfRequests();
				}

				for (int i = 0; i < 6; i++)
				{
					using (var session = store.OpenAsyncSession(new OpenSessionOptions
					{
						ForceReadFromMaster = true
					}))
					{
						Assert.NotNull(await session.LoadAsync<Company>("companies/1"));
					}
				}
			}
			Assert.Equal(6, servers[0].Server.NumberOfRequests);
			Assert.Equal(0, servers[1].Server.NumberOfRequests);
			Assert.Equal(0, servers[2].Server.NumberOfRequests);
		}
	}
}