using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Replication;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CounterLoadBalancingTests : RavenBaseCountersTest
	{
		[Theory]
		[InlineData(6)]
		[InlineData(9)]
		[InlineData(30)]
		public async Task When_replicating_can_do_read_striping(int requestCount)
		{
			using (var serverA = GetNewServer(8077))
			using (var serverB = GetNewServer(8076))
			using (var serverC = GetNewServer(8075))
			{
				using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA))
				using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
				using (var ravenStoreC = NewRemoteDocumentStore(ravenDbServer: serverC))
				{
					using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreA))
					using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreB))
					using (var storeC = NewRemoteCountersStore(DefaultCounteStorageName, ravenStore: ravenStoreC))
					{
						storeA.Convention.FailoverBehavior = FailoverBehavior.ReadFromAllServers;
						await SetupReplicationAsync(storeA, storeB);
						await SetupReplicationAsync(storeA, storeC);

						serverA.Server.ResetNumberOfRequests();
						serverB.Server.ResetNumberOfRequests();
						serverC.Server.ResetNumberOfRequests();

						using (var clientA = storeA.NewCounterClient())
						{							
							for (int i = 0; i < requestCount; i++)
								await clientA.Commands.ChangeAsync("group", "counter", 2);
						}

						serverA.Server.NumberOfRequests.Should().Be(requestCount / 3);
						serverB.Server.NumberOfRequests.Should().Be(requestCount / 3);
						serverC.Server.NumberOfRequests.Should().Be(requestCount / 3);
					}
				}
			}
		}
	}
}
