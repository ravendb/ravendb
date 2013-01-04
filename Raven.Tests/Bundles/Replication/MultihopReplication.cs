using Raven.Abstractions.Replication;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class MultihopReplication : ReplicationBase
	{
		IDocumentStore store1;
		IDocumentStore store2;
		IDocumentStore store3;

		string tracerId;

		[Fact]
		public void Can_run_replication_through_multiple_instances()
		{
			store1 = CreateStore();
			store2 = CreateStore();
			store3 = CreateStore();

			tracerId = WriteTracer(store1);

			WaitForDocument<object>(store1, tracerId);

			RunReplication(store1, store2);
			WaitForDocument<object>(store2, tracerId);

			RunReplication(store2, store3, TransitiveReplicationOptions.Replicate);
			WaitForDocument<object>(store3, tracerId);
		}

		[Fact]
		public void When_source_is_reset_can_replicate_back()
		{
			Can_run_replication_through_multiple_instances();

			store1 = ResetDatabase(0);

			RunReplication(store3, store1, TransitiveReplicationOptions.Replicate);

			WaitForDocument<object>(store1, tracerId);
		}

		private string WriteTracer(IDocumentStore store1)
		{
			var targetStore = store1;
			using(var session = targetStore.OpenSession())
			{
				var tracer = new {};
				session.Store(tracer);
				session.SaveChanges();
				return session.Advanced.GetDocumentId(tracer);
			}
		}
	}
}
