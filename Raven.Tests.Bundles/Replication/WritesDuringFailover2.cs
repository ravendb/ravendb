using System;
using System.Net;
using System.Net.Http;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Bundles.Versioning;
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
			GetReplicationInformer(serverClient).RefreshReplicationInformation(serverClient);

			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			WaitForReplication(store2, "companies/1");

			servers[0].Dispose();

			using (var session = store1.OpenSession())
			{
				Assert.Throws<ErrorResponseException>(() => session.Load<Company>("companies/1"));
			}
		}
	}
}