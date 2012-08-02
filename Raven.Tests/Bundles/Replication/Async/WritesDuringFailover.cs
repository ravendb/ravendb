using System;
using System.Net;
using System.Threading;
using Raven.Bundles.Tests.Versioning;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Xunit;

namespace Raven.Bundles.Tests.Replication.Async
{
	public class WritesDuringFailover : ReplicationBase
	{
		[Fact]
		public void Can_failover_reads()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Company {Name = "Hibernating Rhinos"});
				session.SaveChangesAsync().Wait();
			}

			WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				var company = session.LoadAsync<Company>("companies/1").Result;
				Assert.NotNull(company);
			}
		}

		[Fact]
		public void Can_disallow_failover()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			store1.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChangesAsync().Wait();
			}

			WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				Assert.Throws<AggregateException>(() => session.LoadAsync<Company>("companies/1").Result);
			}
		}

		[Fact]
		public void Cannot_failover_writes_by_default()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			
			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChangesAsync().Wait();
			}

			WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				var company = session.LoadAsync<Company>("companies/1").Result;
				Assert.NotNull(company);
				company.Name = "different";
				AggregateException aggregateException = Assert.Throws<AggregateException>(() => session.SaveChangesAsync().Wait());
				InvalidOperationException invalidOperationException = Assert.IsType<InvalidOperationException>(aggregateException.Flatten().InnerException);
				Assert.Equal("Could not replicate POST operation to secondary node, failover behavior is: AllowReadsFromSecondaries",
					invalidOperationException.Message);
			}
		}

		[Fact]
		public void Can_explicitly_allow_replication()
		{
			var store1 = CreateStore();
			store1.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries;
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);


			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChangesAsync().Wait();
			}

			WaitForReplication(store2);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				var company = session.LoadAsync<Company>("companies/1").Result;
				Assert.NotNull(company);
				company.Name = "different";
				session.SaveChangesAsync().Wait();
			}
		}

		private void WaitForReplication(IDocumentStore store2)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenAsyncSession())
				{
					var company = session.LoadAsync<Company>("companies/1").Result;
					if (company != null)
						break;
					Thread.Sleep(100);
				}
			}
		}
	}
}