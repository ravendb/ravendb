using System;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterFailoverTests : RavenBaseCountersTest
	{
		public class Example
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public async Task Failover_convention_FailImmediately_should_prevent_failover()
		{
			using (var serverA = GetNewServer(8077))
			using (var serverB = GetNewServer(8076))
			{
				var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA);
				try
				{
					using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
					{
						using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreA))
						using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreB))
						{
							storeA.Convention.FailoverBehavior = FailoverBehavior.FailImmediately;
							
							await SetupReplicationAsync(storeA, storeB);

							using (var clientA = storeA.NewCounterClient())
							{
								await clientA.Commands.ChangeAsync("group", "counter", 2);
							}

							await WaitForReplicationBetween(storeA, storeB, "group", "counter");

							using (var clientA = storeA.NewCounterClient())
							{
								serverA.Dispose();
								Assert.Throws<AggregateException>(() => clientA.Commands.GetOverallTotalAsync("group", "counter").Wait());
							}
						}
					}
				}
				finally
				{
					ravenStoreA.Dispose();
				}
			}
		}

	
		[Fact]
		public async Task Two_node_failover_should_work()
		{

			using (var serverA = GetNewServer(8077))
			using (var serverB = GetNewServer(8076))
			{
				var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA);
				try
				{
					using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
					{
						using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreA))
						using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreB))
						{
							await SetupReplicationAsync(storeA, storeB);

							using (var clientA = storeA.NewCounterClient())
							{
								await clientA.Commands.ChangeAsync("group", "counter", 2);
							}

							await WaitForReplicationBetween(storeA, storeB, "group", "counter");

							using (var clientA = storeA.NewCounterClient())
							{
								serverA.Dispose();

								var total = await clientA.Commands.GetOverallTotalAsync("group", "counter");
								Assert.Equal(2, total);
							}
						}
					}
				}
				finally
				{
					ravenStoreA.Dispose();
				}
			}
		}

		[Fact]
		public async Task Multiple_node_failover_should_work()
		{
			var serverA = GetNewServer(8077,runInMemory:false);
			try
			{
				using (var serverB = GetNewServer(8076))
				using (var serverC = GetNewServer(8075))
				{
					var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA, runInMemory:false);
					var ravenStoreC = NewRemoteDocumentStore(ravenDbServer: serverC);
					try
					{
						using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
						{
							using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreA))
							using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreB))
							using (var storeC = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreC))
							{
								await SetupReplicationAsync(storeA, storeB, storeC);

								using (var clientA = storeA.NewCounterClient())
									await clientA.Commands.ChangeAsync("group", "counter", 2);

								Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
								Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));

								using (var clientA = storeA.NewCounterClient())
								{
									storeA.ReplicationInformer.RefreshReplicationInformation(clientA);
									serverA.Dispose();

									var total = await clientA.Commands.GetOverallTotalAsync("group", "counter");
									Assert.Equal(2, total);
								}

								serverA = GetNewServer(8077, runInMemory: false);
																
								using (var clientA = storeA.NewCounterClient())
								{
									storeA.ReplicationInformer.RefreshReplicationInformation(clientA);
									//now both A and B are dead, so check if we can fallback to C as well
									serverA.Dispose();
									serverB.Dispose(); 

									var total = await clientA.Commands.GetOverallTotalAsync("group", "counter");
									Assert.Equal(2, total);
								}
							}
						}
					}
					finally
					{
						ravenStoreA.Dispose();
						ravenStoreC.Dispose();
					}
				}
			}
			finally
			{
				serverA.Dispose();
			}
		}
	}
}
