using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading;
	using System.Threading.Tasks;
	using FluentAssertions;
	using Xunit;

	namespace Raven.Tests.Issues
	{
		public class RavenDB_3540 : ReplicationBase
		{
			private const int clusterSize = 2;

			[Fact]
			public void Delete_in_a_multiple_server_cluster_should_not_cause_infinite_loop()
			{
				var stores = new List<IDocumentStore>();
				CreateCluster(stores);

				stores[0].DatabaseCommands.Put("foo/bar", null, RavenJObject.FromObject(new { Foo = "Bar" }), new RavenJObject());

				stores.ForEach(store => WaitForReplication(store, "foo/bar"));

				stores[0].DatabaseCommands.Delete("foo/bar", null);
				var waitForReplicationTask = Task.Run(() => stores.ForEach(store => WaitForReplication(store, "foo/bar")));
				Task.WaitAny(waitForReplicationTask, Task.Delay(TimeSpan.FromSeconds(5)));

				servers.ForEach(srv => srv.Server.ResetNumberOfRequests());

				for (int i = 0; i < 3; i++)
				{
					Thread.Sleep(300);
					servers.ForEach(srv => srv.Server.NumberOfRequests.Should().Be(0));
				}
			}

			private void CreateCluster(List<IDocumentStore> stores)
			{
				for (int i = 0; i < clusterSize; i++)
					stores.Add(CreateStore());

				stores.ForEach(store => SetupReplication(store.DatabaseCommands, stores.Where(x => x.Identifier != store.Identifier)
																					  .Select(x => RavenJObject.FromObject(new ReplicationDestination
																					  {
																						  Url = x.Url,
																						  TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate,
																					  }))));
			}
		}
	}

}
