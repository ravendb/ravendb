using System;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesFailoverTests : RavenBaseTimeSeriesTest
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
						using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreA))
						using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreB))
						{
							storeA.Convention.FailoverBehavior = FailoverBehavior.FailImmediately;

							await SetupReplicationAsync(storeA, storeB);
							await storeA.ChangeAsync("group", "time series", 2);

							await WaitForReplicationBetween(storeA, storeB, "group", "time series");
							serverA.Dispose();
							Assert.Throws<AggregateException>(() => storeA.GetOverallTotalAsync("group", "time series").Wait());
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
						using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreA))
						using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreB))
						{
							await SetupReplicationAsync(storeA, storeB);
							await storeA.ChangeAsync("group", "time series", 2);

							await WaitForReplicationBetween(storeA, storeB, "group", "time series");

							ravenStoreA.Dispose();
							serverA.Dispose();

							var total = await storeA.GetOverallTotalAsync("group", "time series");
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

		private void SetDisabledStateOnTimeSeries(string name,IDocumentStore store, bool value)
		{
			var timeSeriesDocumentKey = "Raven/TimeSeries/" + name;
			var document = store.DatabaseCommands.ForSystemDatabase().Get(timeSeriesDocumentKey);
			var timeSeriesDocument = document.DataAsJson.ToObject<TimeSeriesDocument>();
			timeSeriesDocument.Disabled = value;

			store.DatabaseCommands.ForSystemDatabase().Put(timeSeriesDocumentKey, null, RavenJObject.FromObject(timeSeriesDocument), new RavenJObject());
		}

		//test to check the following scenario:
		//1) timeSeriesA, timeSeriesB online. 
		//2) do changes in timeSeriesA
		//3) wait for replication timeSeriesA -> timeSeriesB
		//4) timeSeriesA offline, try to read from it (failover to timeSeriesB)
		//5) timeSeriesA becomes online, timeSeriesB offline, try to read from A (failover switches back to timeSeriesA)
		[Fact]
		public async Task Alternating_failover_nodes_should_work()
		{
			using (var serverA = GetNewServer(8077, runInMemory: false))
			using (var serverB = GetNewServer(8078, runInMemory: false))
			using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA, runInMemory:false))
			using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB, runInMemory:false)) 
			using (var storeA = NewRemoteTimeSeriesStore("A", ravenStore: ravenStoreA))
			using (var storeB = NewRemoteTimeSeriesStore("B", ravenStore: ravenStoreB))
			{
				await SetupReplicationAsync(storeA, storeB);
				await storeA.ChangeAsync("group", "time series", 3);
				
				await WaitForReplicationBetween(storeA, storeB, "group", "time series");
				SetDisabledStateOnTimeSeries(storeA.Name, ravenStoreA, true);

				var total = await storeA.GetOverallTotalAsync("group", "time series");
				Assert.Equal(3, total);

				SetDisabledStateOnTimeSeries(storeA.Name, ravenStoreA, false);
				SetDisabledStateOnTimeSeries(storeB.Name, ravenStoreB, true);

				storeA.ReplicationInformer.RefreshReplicationInformation();

				total = await storeA.GetOverallTotalAsync("group", "time series");
				Assert.Equal(3, total);

				SetDisabledStateOnTimeSeries(storeB.Name, ravenStoreB, false);
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
			using (var storeA = NewRemoteTimeSeriesStore("A", ravenStore: ravenStoreA))
			using (var storeB = NewRemoteTimeSeriesStore("B", ravenStore: ravenStoreB))
			using (var storeC = NewRemoteTimeSeriesStore("C", ravenStore: ravenStoreC))
			{
				await SetupReplicationAsync(storeA, storeB, storeC);
				await storeA.ChangeAsync("group", "time series", 2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "time series"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "time series"));

				SetDisabledStateOnTimeSeries(storeA.Name, ravenStoreA, true);
				
				storeA.ReplicationInformer.RefreshReplicationInformation();
				
				//A is dead -> checking if we can fall back to B or C
				var total = await storeA.GetOverallTotalAsync("group", "time series");
				Assert.Equal(2, total);

				SetDisabledStateOnTimeSeries(storeB.Name, ravenStoreB, true);

				storeA.ReplicationInformer.RefreshReplicationInformation();

				//now B is also dead, make sure we can fall back on C
				total = await storeA.GetOverallTotalAsync("group", "time series");
				Assert.Equal(2, total);
			}
		}

		//this test is intentionally has almost the same code as Multiple_node_failover_should_work
		//the difference is absense of calls ITimeSeriesStore::ReplicationInformer::RefreshReplicationInformation()
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
			using (var storeA = NewRemoteTimeSeriesStore("A", ravenStore: ravenStoreA))
			using (var storeB = NewRemoteTimeSeriesStore("B", ravenStore: ravenStoreB))
			using (var storeC = NewRemoteTimeSeriesStore("C", ravenStore: ravenStoreC))
			{
				await SetupReplicationAsync(storeA, storeB, storeC);
				await storeA.ChangeAsync("group", "time series", 2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "time series"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "time series"));

				storeA.ReplicationInformer.MaxIntervalBetweenUpdatesInMilliseconds = 0; 

				SetDisabledStateOnTimeSeries(storeA.Name, ravenStoreA, true);


				//A is dead -> checking if we can fall back to B or C
				var total = await storeA.GetOverallTotalAsync("group", "time series");
				Assert.Equal(2, total);

				SetDisabledStateOnTimeSeries(storeB.Name, ravenStoreB, true);

				//now B is also dead, make sure we can fall back on C
				total = await storeA.GetOverallTotalAsync("group", "time series");
				Assert.Equal(2, total);
			}
		}
	}
}