using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterReplicationTests : RavenBaseCountersTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			base.ModifyConfiguration(configuration);
			configuration.Settings[Constants.Counter.ReplicationLatencyMs] = "10";
			configuration.Counter.ReplicationLatencyInMs = 10;
		}

		[Fact]
		public async Task Replication_setup_should_work()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);

				var replicationDocument = await storeA.GetReplicationsAsync();

                Assert.Equal(1, replicationDocument.Destinations.Count);
                Assert.Equal(storeB.Url, replicationDocument.Destinations[0].ServerUrl);
			}
		}

		//simple case
		[Fact]
		public async Task Simple_replication_should_work()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				
				await storeA.ChangeAsync("group", "counter", 2);
				
				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
			}
		}

		[Fact]
		public async Task Simple_replication_should_work2()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);

				await storeA.ChangeAsync("group", "counter", 2);
				await storeA.ChangeAsync("group", "counter", -1);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
			}
		}

		//2 way replication
		[Fact]
		public async Task Two_way_replication_should_work2()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				await storeA.ChangeAsync("group", "counter", 2);
				await storeB.ChangeAsync("group", "counter", 3);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
			}
		}

		[Fact]
		public async Task Two_way_replication_should_work3()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				await storeA.ChangeAsync("group", "counter", 2);
				await storeA.ChangeAsync("group", "counter", -1);
				await storeB.ChangeAsync("group", "counter", -3);
				await storeB.ChangeAsync("group", "counter", 2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
			}
		}

		//more complicated case
		[Fact]
		public async Task Multiple_replication_should_Work()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
			using (var storeC = NewRemoteCountersStore(DefaultCounterStorageName + "C"))
			{
				await SetupReplicationAsync(storeA, storeB, storeC);
				await SetupReplicationAsync(storeB, storeA, storeC);
				await SetupReplicationAsync(storeC, storeA, storeB);				

				await storeA.ChangeAsync("group", "counter", 1);
	
				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeB, storeC, "group", "counter"));

				await storeB.ChangeAsync("group", "counter", -2);
	
				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));

				await storeC.ChangeAsync("group", "counter", 1);
	
				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));
			}
		}

	}
}
