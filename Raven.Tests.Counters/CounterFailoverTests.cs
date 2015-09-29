using System;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Database.Extensions;
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
						using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreA))
						using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreB))
						{
							storeA.CountersConvention.FailoverBehavior = FailoverBehavior.FailImmediately;

							await SetupReplicationAsync(storeA, storeB);
							await storeA.ChangeAsync("group", "counter", 2);

							await WaitForReplicationBetween(storeA, storeB, "group", "counter");
							serverA.Dispose();
							Assert.Throws<AggregateException>(() => storeA.GetOverallTotalAsync("group", "counter").Wait());
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
						using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreA))
						using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName, ravenStore: ravenStoreB))
						{
							await SetupReplicationAsync(storeA, storeB);
							await storeA.ChangeAsync("group", "counter", 2);

							await WaitForReplicationBetween(storeA, storeB, "group", "counter");

							ravenStoreA.Dispose();
							serverA.Dispose();

							var total = await storeA.GetOverallTotalAsync("group", "counter");
							Assert.Equal(2, total);
						}
					}
				}
				finally
				{
					ravenStoreA.Dispose();
				}
			}
		}

		private void SetDisabledStateOnCounter(string name,IDocumentStore store, bool value)
		{
			var counterDocumentKey = "Raven/Counters/" + name;
			var document = store.DatabaseCommands.ForSystemDatabase().Get(counterDocumentKey);
			var counterDocument = document.DataAsJson.ToObject<CounterStorageDocument>();
			counterDocument.Disabled = value;

			store.DatabaseCommands.ForSystemDatabase().Put(counterDocumentKey, null, RavenJObject.FromObject(counterDocument), new RavenJObject());
		}

		//test to check the following scenario:
		//1) counterA, counterB online. 
		//2) do changes in counterA
		//3) wait for replication counterA -> counterB
		//4) counterA offline, try to read from it (failover to counterB)
		//5) counterA becomes online, counterB offline, try to read from A (failover switches back to counterA)
		[Fact]
		public async Task Alternating_failover_nodes_should_work()
		{
			using (var serverA = GetNewServer(8077, runInMemory: false))
			using (var serverB = GetNewServer(8078, runInMemory: false))
			using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA, runInMemory: false))
			using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB, runInMemory: false))
			using (var storeA = NewRemoteCountersStore("A", ravenStore: ravenStoreA))
			using (var storeB = NewRemoteCountersStore("B", ravenStore: ravenStoreB))
			{
				await SetupReplicationAsync(storeA, storeB);
				await storeA.ChangeAsync("group", "counter", 3);

				await WaitForReplicationBetween(storeA, storeB, "group", "counter");
				SetDisabledStateOnCounter(storeA.Name, ravenStoreA, true);

				var total = await storeA.GetOverallTotalAsync("group", "counter");
				Assert.Equal(3, total);

				SetDisabledStateOnCounter(storeA.Name, ravenStoreA, false);
				SetDisabledStateOnCounter(storeB.Name, ravenStoreB, true);

				storeA.ReplicationInformer.RefreshReplicationInformation();

				total = await storeA.GetOverallTotalAsync("group", "counter");
				Assert.Equal(3, total);

				SetDisabledStateOnCounter(storeB.Name, ravenStoreB, false);
			}
		}

		[Fact]
		public async Task Multiple_node_failover_should_work()
		{

			using (var serverA = GetNewServer(8070, runInMemory: false))
			using (var serverB = GetNewServer(8071, runInMemory: false))
			using (var serverC = GetNewServer(8072, runInMemory: false))
			using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA, runInMemory: false))
			using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB, runInMemory: false))
			using (var ravenStoreC = NewRemoteDocumentStore(ravenDbServer: serverC, runInMemory: false))
			using (var storeA = NewRemoteCountersStore("A", ravenStore: ravenStoreA))
			using (var storeB = NewRemoteCountersStore("B", ravenStore: ravenStoreB))
			using (var storeC = NewRemoteCountersStore("C", ravenStore: ravenStoreC))
			{
				await SetupReplicationAsync(storeA, storeB, storeC);
				await storeA.ChangeAsync("group", "counter", 2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));

				SetDisabledStateOnCounter(storeA.Name, ravenStoreA, true);
				
				storeA.ReplicationInformer.RefreshReplicationInformation();
				
				//A is dead -> checking if we can fall back to B or C
				var total = await storeA.GetOverallTotalAsync("group", "counter");
				Assert.Equal(2, total);

				SetDisabledStateOnCounter(storeB.Name, ravenStoreB, true);

				storeA.ReplicationInformer.RefreshReplicationInformation();

				//now B is also dead, make sure we can fall back on C
				total = await storeA.GetOverallTotalAsync("group", "counter");
				Assert.Equal(2, total);
			}
		}

		//this test is intentionally has almost the same code as Multiple_node_failover_should_work
		//the difference is absense of calls ICountersStore::ReplicationInformer::RefreshReplicationInformation()
		//and storeA.ReplicationInformer.MaxIntervalBetweenUpdatesInMillisec = 0; 
		//
		//the idea of this test is to check whether the "auto-update" of replication information works
		[Fact]
		public async Task Updating_replication_information_should_happen_per_specified_intervals()
		{
			using (var serverA = GetNewServer(8070, runInMemory: false))
			using (var serverB = GetNewServer(8071, runInMemory: false))
			using (var serverC = GetNewServer(8072, runInMemory: false))
			using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA, runInMemory: false))
			using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB, runInMemory: false))
			using (var ravenStoreC = NewRemoteDocumentStore(ravenDbServer: serverC, runInMemory: false))
			using (var storeA = NewRemoteCountersStore("A", ravenStore: ravenStoreA))
			using (var storeB = NewRemoteCountersStore("B", ravenStore: ravenStoreB))
			using (var storeC = NewRemoteCountersStore("C", ravenStore: ravenStoreC))
			{
				await SetupReplicationAsync(storeA, storeB, storeC);
				await storeA.ChangeAsync("group", "counter", 2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));

				storeA.ReplicationInformer.MaxIntervalBetweenUpdatesInMilliseconds = 0; 

				SetDisabledStateOnCounter(storeA.Name, ravenStoreA, true);


				//A is dead -> checking if we can fall back to B or C
				var total = await storeA.GetOverallTotalAsync("group", "counter");
				Assert.Equal(2, total);

				SetDisabledStateOnCounter(storeB.Name, ravenStoreB, true);

				//now B is also dead, make sure we can fall back on C
				total = await storeA.GetOverallTotalAsync("group", "counter");
				Assert.Equal(2, total);
			}
		}
	}
}