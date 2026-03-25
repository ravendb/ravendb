using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Metrics
{
    public class RequestRouterMetricsTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public async Task ThroughputIsRecordedOnCompletion()
        {
            using var store = GetDocumentStore();

            // Warm-up to make sure the database is created.
            await store.Maintenance.SendAsync(new GetStatisticsOperation());

            var database = await GetDocumentDatabaseInstanceForAsync(store.Database);

            var initialCount = database.Metrics.Requests.RequestsPerSec.Count;

            await store.Maintenance.SendAsync(new GetStatisticsOperation());

            await AssertWaitForGreaterThanAsync(() => Task.FromResult(database.Metrics.Requests.RequestsPerSec.Count), initialCount);
        }
    }
}
