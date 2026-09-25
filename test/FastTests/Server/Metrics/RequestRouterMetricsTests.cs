using System.Net.Http;
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

        [RavenFact(RavenTestCategory.Core)]
        public async Task EachRequestIsRecordedOnce()
        {
            const long numberOfRequests = 10;

            using var store = GetDocumentStore();
            var database = await GetDocumentDatabaseInstanceForAsync(store.Database);
            var initialCount = database.Metrics.Requests.RequestsPerSec.Count;

            // reading a response to the end means the server already left RequestHandler, so every metric update for it is done
            using var client = new HttpClient();
            for (var i = 0; i < numberOfRequests; i++)
                await client.GetStringAsync($"{store.Urls[0]}/databases/{store.Database}/stats");

            Assert.Equal(numberOfRequests, database.Metrics.Requests.RequestsPerSec.Count - initialCount);
        }
    }
}
