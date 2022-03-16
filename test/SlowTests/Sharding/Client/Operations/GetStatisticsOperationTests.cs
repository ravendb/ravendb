using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations;

public class GetStatisticsOperationTests : RavenTestBase
{
    public GetStatisticsOperationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task CanGetStatistics(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < 20; i++)
                    await session.StoreAsync(new User { Age = i });

                await session.SaveChangesAsync();
            }

            var statistics0 = await store.Maintenance.ForNode("A").ForShardWithProxy(0).SendAsync(new GetStatisticsOperation());
            Assert.NotNull(statistics0);
            Assert.NotEqual(0, statistics0.CountOfDocuments);

            var statistics1 = await store.Maintenance.ForNode("A").ForShardWithProxy(1).SendAsync(new GetStatisticsOperation());
            Assert.NotNull(statistics1);
            Assert.NotEqual(0, statistics1.CountOfDocuments);

            var statistics2 = await store.Maintenance.ForNode("A").ForShardWithProxy(2).SendAsync(new GetStatisticsOperation());
            Assert.NotNull(statistics2);
            Assert.NotEqual(0, statistics2.CountOfDocuments);

            var total = statistics0.CountOfDocuments + statistics1.CountOfDocuments + statistics2.CountOfDocuments;

            Assert.Equal(20 + 1, total); // +1 for hilo
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task GetStatistics_ShouldThrow(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.SendAsync(new GetStatisticsOperation()));
            Assert.Contains("Query string nodeTag is mandatory, but wasn't specified", e.Message);

            e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.ForNode("A").SendAsync(new GetStatisticsOperation()));
            Assert.Contains("Query string nodeTag is mandatory, but wasn't specified", e.Message);

            e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.SendAsync(new GetDetailedStatisticsOperation()));
            Assert.Contains("Query string nodeTag is mandatory, but wasn't specified", e.Message);

            e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.ForNode("A").SendAsync(new GetDetailedStatisticsOperation()));
            Assert.Contains("Query string nodeTag is mandatory, but wasn't specified", e.Message);
        }
    }
}
