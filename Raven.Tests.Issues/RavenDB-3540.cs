using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3540 : ReplicationBase
	{
		private const int clusterSize = 3;

		[Fact]
		public void Delete_in_a_multiple_server_cluster_should_not_cause_infinite_loop()
		{
			var stores = new List<DocumentStore>();
			CreateCluster(stores);
		}

		private void CreateCluster(List<DocumentStore> stores)
		{
			for (int i = 0; i < clusterSize; i++)
				stores.Add(CreateStore());

			stores.ForEach(store => SetupReplication(store.DatabaseCommands,stores.ToArray()));
		}
	}
}
