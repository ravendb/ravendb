using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class WritesDuringFailover2 : ReplicationBase
	{
		protected override void ModifyStore(DocumentStore documentStore)
		{
			documentStore.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
		}

		[Fact]
		public void Can_disallow_failover()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			var replicationInformer = GetReplicationInformer(serverClient);
			replicationInformer.RefreshReplicationInformation(serverClient);
			var dest = replicationInformer.ReplicationDestinations;
			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			WaitForReplication(store2, "companies/1");

			servers[0].Dispose();
			dest = replicationInformer.ReplicationDestinations;
			using (var session = store1.OpenSession())
			{
				Assert.Throws<ErrorResponseException>(() => session.Load<Company>("companies/1"));
			}
		}
	}
}