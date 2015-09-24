// -----------------------------------------------------------------------
//  <copyright file="CountersTopologyViewTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Counters;
using Raven.Client.Document;
using Raven.Database.Bundles.Replication.Data;
using Raven.Database.Counters.Replication;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CountersTopologyViewTests : RavenBaseCountersTest
	{
		public class WithoutAuth : CountersTopologyViewTests
		{
			[Fact]
			public async Task SynchronizationTopologyDiscovererSimpleTest()
			{
				using (CounterStore storeA = (CounterStore) NewRemoteCountersStore(DefaultCounterStorageName + "A"))
				using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
				using (var storeC = NewRemoteCountersStore(DefaultCounterStorageName + "C"))
				using (var storeD = NewRemoteCountersStore(DefaultCounterStorageName + "D"))
				using (var storeE = NewRemoteCountersStore(DefaultCounterStorageName + "E"))
				{
					await SetupReplicationAsync(storeA, storeB);
					await SetupReplicationAsync(storeB, storeC);
					await SetupReplicationAsync(storeC, storeD);
					await SetupReplicationAsync(storeD, storeE);
					await SetupReplicationAsync(storeE, storeA);

					await storeA.ChangeAsync("group", "counter", 2);
					await WaitForReplicationBetween(storeA, storeE, "group", "counter");

					var url = storeA.Url.ForCounter(DefaultCounterStorageName + "A1") + "/admin/replication/topology/view";

					var request = storeA
						.JsonRequestFactory
						.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Post, storeA.Credentials, storeA.CountersConvention));

					var json = (RavenJObject)request.ReadResponseJson();
					var topology = json.Deserialize<CountersReplicationTopology>(new DocumentConvention());

					Assert.NotNull(topology);
					Assert.Equal(5, topology.Servers.Count);
					Assert.Equal(5, topology.Connections.Count);

					
					topology.Connections.Single(x => x.Destination == storeA.Url.ForCounter(storeA.Name) && x.Source == storeE.Url.ForCounter(storeE.Name));
					topology.Connections.Single(x => x.Destination == storeB.Url.ForCounter(storeB.Name) && x.Source == storeA.Url.ForCounter(storeA.Name));
					topology.Connections.Single(x => x.Destination == storeC.Url.ForCounter(storeC.Name) && x.Source == storeB.Url.ForCounter(storeB.Name));
					topology.Connections.Single(x => x.Destination == storeD.Url.ForCounter(storeD.Name) && x.Source == storeC.Url.ForCounter(storeC.Name));
					topology.Connections.Single(x => x.Destination == storeE.Url.ForCounter(storeE.Name) && x.Source == storeD.Url.ForCounter(storeD.Name));

					foreach (var connection in topology.Connections)
					{
						Assert.Equal(ReplicatonNodeState.Online, connection.SourceToDestinationState);
						Assert.Equal(ReplicatonNodeState.Online, connection.DestinationToSourceState);
						Assert.NotNull(connection.Source);
						Assert.NotNull(connection.Destination);
						Assert.NotNull(connection.LastEtag);
						Assert.NotNull(connection.SendServerId);
						Assert.NotNull(connection.StoredServerId);
					}
				}
			}
		}
	}
}