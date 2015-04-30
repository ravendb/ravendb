using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Raven.Client.Counters;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterReplicationTests : RavenBaseCountersTest
	{
		[Fact]
		public async Task Replication_setup_should_work()
		{
			using (var storeA = NewRemoteCountersStore())
			using (var storeB = NewRemoteCountersStore())
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

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));
			}			
		}

	}
}
