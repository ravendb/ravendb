using System.Linq;
using Raven.Client.Connection;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class FailoverDisabled : ReplicationBase
	{
		[Fact]
		public void CanDisableFailoverByDisablingDestination()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			RunReplication(store1, store2, disabled: true);
			RunReplication(store1, store3, disabled: false);

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			Assert.Equal(1, serverClient.ReplicationInformer.ReplicationDestinationsUrls.Count());
			Assert.Equal(store3.Url, serverClient.ReplicationInformer.ReplicationDestinationsUrls[0]);
		}

		[Fact]
		public void CanDisableFailoverByDisablingDestinationOnClientOnly()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			RunReplication(store1, store2, ignoredClient: true);
			RunReplication(store1, store3, ignoredClient: false);

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			Assert.Equal(1, serverClient.ReplicationInformer.ReplicationDestinationsUrls.Count());
			Assert.Equal(store3.Url, serverClient.ReplicationInformer.ReplicationDestinationsUrls[0]);
		}
	}
}
