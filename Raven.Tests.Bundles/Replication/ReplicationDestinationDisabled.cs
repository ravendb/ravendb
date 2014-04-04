using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class ReplicationDestinationDisabled : ReplicationBase
	{
		private const int RetriesForDisabledDestination = 30;

		[Fact]
		public async Task CanDisableReplicationToDestination()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			RunReplication(store1, store2, disabled: true);

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Item());
				await session.SaveChangesAsync();
			}

			JsonDocument item = null;
			for (int i = 0; i < RetriesForDisabledDestination; i++)
			{
				item = await store2.AsyncDatabaseCommands.GetAsync("items/1");
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