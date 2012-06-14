using System;
using System.Net;
using System.Threading;
using Raven.Bundles.Tests.Versioning;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Xunit;

namespace Raven.Bundles.Tests.Replication
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

			using (var session = store1.OpenSession())
			{
				session.Store(new Company {Name = "Hibernating Rhinos"});
				session.SaveChanges();
			}

			WaitForReplication(store2, "companies/1");

			servers[0].Dispose();

			using (var session = store1.OpenSession())
			{
				var company = session.Load<Company>("companies/1");
				Assert.NotNull(company);
			}
		}

		[Fact]
		public void Can_disallow_failover()
		{
			var store1 = CreateStore(store => store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately);
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			WaitForReplication(store2, "companies/1");

			servers[0].Dispose();

			using (var session = store1.OpenSession())
			{
				Assert.Throws<WebException>(() => session.Load<Company>("companies/1"));
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

			
			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			WaitForReplication(store2, "companies/1");

			servers[0].Dispose();

			using (var session = store1.OpenSession())
			{
				var company = session.Load<Company>("companies/1");
				Assert.NotNull(company);
				company.Name = "different";
				InvalidOperationException invalidOperationException = Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
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


			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			WaitForReplication(store2, "companies/1");

			servers[0].Dispose();

			using (var session = store1.OpenSession())
			{
				var company = session.Load<Company>("companies/1");
				Assert.NotNull(company);
				company.Name = "different";
				session.SaveChanges();
			}
		}

		
	}
}
