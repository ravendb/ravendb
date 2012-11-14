extern alias database;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
	public class ReplicationDestinationDisabled : ReplicationBase
	{
		private const int RetriesForDisabledDestination = 30;

		[Fact]
		public void CanDisableReplicationToDestination()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			RunReplication(store1, store2, disabled: true);

			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Item());
				session.SaveChangesAsync().Wait();
			}

			JsonDocument item = null;
			for (int i = 0; i < RetriesForDisabledDestination; i++)
			{
				item = store2.DatabaseCommands.Get("items/1");
				if (item != null)
					break;
				Thread.Sleep(100);
			}
			Assert.Null(item);
		}

		public class Item
		{
		}
	}
}