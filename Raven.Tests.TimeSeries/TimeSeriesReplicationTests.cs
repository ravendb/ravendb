using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesReplicationTests : RavenBaseTimeSeriesTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			base.ModifyConfiguration(configuration);
			configuration.Settings[Constants.TimeSeries.ReplicationLatencyMs] = "10";
			configuration.TimeSeries.ReplicationLatencyInMs = 10;
		}

		[Fact]
		public async Task Replication_setup_should_work()
		{
			using (var storeA = NewRemoteTimeSeriesStore(port: 8079))
			using (var storeB = NewRemoteTimeSeriesStore(port: 8078))
			{
				await SetupReplicationAsync(storeA, storeB);

				var replicationDocument = await storeA.GetReplicationsAsync();

				Assert.Equal(1, replicationDocument.Destinations.Count);
				Assert.Equal(storeB.Url, replicationDocument.Destinations[0].ServerUrl);
			}
		}

		//simple case
		[Fact(Skip = "Doesn't work, need to setup replication")]
		public async Task Simple_replication_should_work()
		{
			using (var storeA = NewRemoteTimeSeriesStore(port: 8079))
			using (var storeB = NewRemoteTimeSeriesStore(port: 8078))
			{
				await SetupReplicationAsync(storeA, storeB);

				await storeA.CreateTypeAsync("SmartWatch", new [] { "Heartrate", "Geo Latitude", "Geo Longitude" });
				await storeA.AppendAsync("SmartWatch", "Watch-123456", DateTimeOffset.UtcNow, new [] { 111d, 222d, 333d });

				await storeA.WaitForReplicationAsync();
			}
		}

		/*[Fact]
		public async Task Simple_replication_should_work2()
		{
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriestorageName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriestorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);

				await storeA.ChangeAsync("group", "TimeSeries", 2);
				await storeA.ChangeAsync("group", "TimeSeries", -1);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "TimeSeries"));
			}
		}

		//2 way replication
		[Fact]
		public async Task Two_way_replication_should_work2()
		{
			using (var storeA = NewRemoteTimeSeriesStore(port: 8079))
			using (var storeB = NewRemoteTimeSeriesStore(port: 8078))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				await storeA.ChangeAsync("group", "TimeSeries", 2);
				await storeB.ChangeAsync("group", "TimeSeries", 3);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "TimeSeries"));
			}
		}

		[Fact]
		public async Task Two_way_replication_should_work3()
		{
			using (var storeA = NewRemoteTimeSeriesStore(port: 8079))
			using (var storeB = NewRemoteTimeSeriesStore(port: 8078))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				await storeA.ChangeAsync("group", "TimeSeries", 2);
				await storeA.ChangeAsync("group", "TimeSeries", -1);
				await storeB.ChangeAsync("group", "TimeSeries", -3);
				await storeB.ChangeAsync("group", "TimeSeries", 2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "TimeSeries"));
			}
		}

		//more complicated case
		[Fact]
		public async Task Multiple_replication_should_Work()
		{
			using (var storeA = NewRemoteTimeSeriesStore(port: 8079))
			using (var storeB = NewRemoteTimeSeriesStore(port: 8078))
			using (var storeC = NewRemoteTimeSeriesStore(port: 8077))
			{
				await SetupReplicationAsync(storeA, storeB, storeC);
				await SetupReplicationAsync(storeB, storeA, storeC);
				await SetupReplicationAsync(storeC, storeA, storeB);

				await storeA.ChangeAsync("group", "TimeSeries", 1);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "TimeSeries"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "TimeSeries"));
				Assert.True(await WaitForReplicationBetween(storeB, storeC, "group", "TimeSeries"));

				await storeB.ChangeAsync("group", "TimeSeries", -2);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "TimeSeries"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "TimeSeries"));

				await storeC.ChangeAsync("group", "TimeSeries", 1);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "TimeSeries"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "TimeSeries"));
			}
		}*/
	}
}