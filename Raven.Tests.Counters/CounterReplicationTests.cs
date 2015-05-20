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

				using (var client = storeA.NewCounterClient())
				{
					var replicationDocument = await client.Replication.GetReplicationsAsync();

					replicationDocument.Destinations.Should().OnlyContain(dest => dest.ServerUrl == storeB.Url);
				}
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

				using (var client = storeA.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter",2);
				}
				
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

				using (var client = storeA.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter", 2);
				}

				using (var client = storeB.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter", 3);
				}

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

				using (var client = storeA.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter", 2);
				}

				using (var client = storeA.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter", -1);
				}

				using (var client = storeA.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter", 4);
				}

				using (var client = storeB.NewCounterClient())
				{
					await client.Commands.ChangeAsync("group", "counter", 4);
				}

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));
			}			
		}

	}
}
