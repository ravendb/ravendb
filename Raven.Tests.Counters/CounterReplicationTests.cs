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
		public async Task Two_node_replication_should_work()
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

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "counter"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "counter"));
			}			
		}

		private async Task<bool> WaitForReplicationBetween(ICounterStore source,ICounterStore destination, string groupName, string counterName, int timeoutInSec = 30)
		{
			var waitStartingTime = DateTime.Now;
			var hasReplicated = false;

			if (Debugger.IsAttached)
				timeoutInSec = 60*60; //1 hour timeout if debugging

			while (true)
			{
				if ((DateTime.Now - waitStartingTime).TotalSeconds > timeoutInSec)
					break;

				using(var sourceClient = source.NewCounterClient())
				using (var destinationClient = destination.NewCounterClient())
				{
					var sourceValue = await sourceClient.Commands.GetOverallTotalAsync(groupName, counterName);
					var targetValue = await destinationClient.Commands.GetOverallTotalAsync(groupName, counterName);
					if (sourceValue == targetValue)
					{
						hasReplicated = true;
						break;
					}
				}

				Thread.Sleep(50);
			}

			return hasReplicated;
		}

		private async Task SetupReplicationAsync(ICounterStore source, params ICounterStore[] destinations)
		{
			using (var client = source.NewCounterClient())
			{
				var replicationDocument = new CountersReplicationDocument();
				foreach (var destStore in destinations)
				{
					using (var destClient = destStore.NewCounterClient())
					{
						replicationDocument.Destinations.Add(new CounterReplicationDestination
						{
							CounterStorageName = destClient.CounterStorageName,
							ServerUrl = destClient.ServerUrl							
						});

					}
				}

				await client.Replication.SaveReplicationsAsync(replicationDocument);
			}
		}	
	}
}
