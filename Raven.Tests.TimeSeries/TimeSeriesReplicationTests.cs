/*
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesReplicationTests : RavenBaseTimeSeriesTest
	{
		[Fact]
		public async Task Replication_setup_should_work()
		{
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "B"))
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
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				
				await storeA.ChangeAsync("group", "time series", 2);
				
				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "time series"));
			}
		}

		//2 way replication
		[Fact]
		public async Task Two_way_replication_should_work2()
		{
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				await storeA.ChangeAsync("group", "time series", 2);
				await storeB.ChangeAsync("group", "time series", 3);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "time series"));
			}
		}

		//more complicated case
		[Fact]
		public async Task Multiple_replication_should_Work()
		{
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "B"))
			using (var storeC = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "C"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeA, storeC);
				await SetupReplicationAsync(storeB, storeA);
				await SetupReplicationAsync(storeB, storeC);
				await SetupReplicationAsync(storeC, storeA);
				await SetupReplicationAsync(storeC, storeB);

				await storeA.ChangeAsync("group", "time series", 2);

				await storeA.ChangeAsync("group", "time series", -1);

				await storeA.ChangeAsync("group", "time series", 4);

				await storeB.ChangeAsync("group", "time series", 4);

				Assert.True(await WaitForReplicationBetween(storeA, storeB, "group", "time series"));
				Assert.True(await WaitForReplicationBetween(storeA, storeC, "group", "time series"));
			}
		}

	}
}
*/
