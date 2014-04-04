using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Async
{
	public class WritesDuringFailover : ReplicationBase
	{
		[Fact]
		public async Task Can_failover_reads()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company {Name = "Hibernating Rhinos"});
				await session.SaveChangesAsync();
			}

			await WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				var company = await session.LoadAsync<Company>("companies/1");
				Assert.NotNull(company);
			}
		}

		[Fact]
		public async Task Can_disallow_failover()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			store1.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				await session.SaveChangesAsync();
			}

			await WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
                await AssertAsync.Throws<HttpRequestException>(async () => await session.LoadAsync<Company>("companies/1"));
			}
		}

		[Fact]
		public async Task Cannot_failover_writes_by_default()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);
			
			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				await session.SaveChangesAsync();
			}

			await WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				var company = await session.LoadAsync<Company>("companies/1");
				Assert.NotNull(company);
				company.Name = "different";
                var invalidOperationException = await AssertAsync.Throws<InvalidOperationException>(async () => await session.SaveChangesAsync());
				Assert.Equal("Could not replicate POST operation to secondary node, failover behavior is: AllowReadsFromSecondaries",
					invalidOperationException.Message);
			}
		}

		[Fact]
		public async Task Can_explicitly_allow_replication()
		{
			var store1 = CreateStore();
			store1.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries;
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);


			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				await session.SaveChangesAsync();
			}

			await WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				var company = await session.LoadAsync<Company>("companies/1");
				Assert.NotNull(company);
				company.Name = "different";
				await session.SaveChangesAsync();
			}
		}

		private async Task WaitForReplication(IDocumentStore store2)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenAsyncSession())
				{
					var company = await session.LoadAsync<Company>("companies/1");
					if (company != null)
						break;
					Thread.Sleep(100);
				}
			}
		}
	}
}