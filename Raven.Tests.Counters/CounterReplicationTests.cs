using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterReplicationTests : RavenBaseCountersTest
	{
		[Fact]
		public async Task Replication_setup_should_work()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);

				var replicationDocument = await storeA.GetReplicationsAsync();

				replicationDocument.Destinations.Should().OnlyContain(dest => dest.ServerUrl == storeB.Url);
			}
		}

		//simple case
		[Fact]
		public async Task Simple_replication_should_work()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				
				await storeA.ChangeAsync("group", "counter", 2);
				
				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
			}
		}

		//2 way replication
		[Fact]
		public async Task Two_way_replication_should_work2()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				await storeA.ChangeAsync("group", "counter", 2);
				await storeB.ChangeAsync("group", "counter", 3);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
			}
		}

		//more complicated case
		[Fact]
		public async Task Multiple_replication_should_Work()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName + "B"))
			using (var storeC = NewRemoteCountersStore(DefaultCounteStorageName + "C"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeA, storeC);
				await SetupReplicationAsync(storeB, storeA);
				await SetupReplicationAsync(storeB, storeC);
				await SetupReplicationAsync(storeC, storeA);
				await SetupReplicationAsync(storeC, storeB);

				await storeA.ChangeAsync("group", "counter", 2);

				await storeA.ChangeAsync("group", "counter", -1);

				await storeA.ChangeAsync("group", "counter", 4);

				await storeB.ChangeAsync("group", "counter", 4);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));
			}
		}

	}
}
