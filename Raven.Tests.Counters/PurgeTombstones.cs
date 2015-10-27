using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Counters;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Counters
{
	public class PurgeTombstones : RavenBaseCountersTest
	{
		private const string CounterStorageName = "cs1";

		[Fact]
		public async Task simple_purge_tombstones()
		{
			using (var counterStore = NewRemoteCountersStore(CounterStorageName))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				var stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 1);
				Assert.Equal(stats.TombstonesCount, 0);

				await counterStore.DeleteAsync("g1", "c1");
				stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 0);
				Assert.Equal(stats.TombstonesCount, 1);

				var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
																			  servers[0].SystemDatabase.ServerUrl +
																			  string.Format("cs/{0}/purge-tombstones", counterStore.Name),
																			  HttpMethods.Post,
																			  new OperationCredentials(null, CredentialCache.DefaultCredentials),
																			  counterStore.CountersConvention);

				counterStore.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();

				stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 0);
				Assert.Equal(stats.TombstonesCount, 0);
			}
		}

		[Fact]
		public async Task should_be_able_to_purge_two_tombstones()
		{
			using (var counterStore = NewRemoteCountersStore(CounterStorageName))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.ChangeAsync("g2", "c2", 5);
				var stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 2);
				Assert.Equal(stats.TombstonesCount, 0);

				await counterStore.DeleteAsync("g1", "c1");
				stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 1);
				Assert.Equal(stats.TombstonesCount, 1);

				await counterStore.DeleteAsync("g2", "c2");
				stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 0);
				Assert.Equal(stats.TombstonesCount, 2);

				var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
																			  servers[0].SystemDatabase.ServerUrl +
																			  string.Format("cs/{0}/purge-tombstones", counterStore.Name),
																			  HttpMethods.Post,
																			  new OperationCredentials(null, CredentialCache.DefaultCredentials),
																			  counterStore.CountersConvention);

				counterStore.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ExecuteRequest();

				stats = await counterStore.GetCounterStatsAsync();
				Assert.Equal(stats.CountersCount, 0);
				Assert.Equal(stats.TombstonesCount, 0);
			}
		}
	}
}
