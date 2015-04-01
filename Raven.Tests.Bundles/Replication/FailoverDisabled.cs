using System.Linq;
using Raven.Client.Connection;
using Raven.Tests.Common;

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
			GetReplicationInformer(serverClient).RefreshReplicationInformation(serverClient);

			Assert.Equal(1, GetReplicationInformer(serverClient).ReplicationDestinationsUrls.Count());

			var expectedDestinationUrl = GetReplicationInformer(serverClient).ReplicationDestinationsUrls[0].Url;
			Assert.Equal(store3.Url + "/databases/" + store3.DefaultDatabase + "/", expectedDestinationUrl);
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
			GetReplicationInformer(serverClient).RefreshReplicationInformation(serverClient);

			Assert.Equal(1, GetReplicationInformer(serverClient).ReplicationDestinationsUrls.Count());
			Assert.Equal(store3.Url + "/databases/" + store3.DefaultDatabase + "/", GetReplicationInformer(serverClient).ReplicationDestinationsUrls[0].Url);
		}
	}
}
