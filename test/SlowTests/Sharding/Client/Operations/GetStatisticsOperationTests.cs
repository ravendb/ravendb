using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations;

public class GetStatisticsOperationTests : RavenTestBase
{
    public GetStatisticsOperationTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task CanGetStatistics()
    {
        using (var store = Sharding.GetDocumentStore())
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

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task GetStatistics_ShouldThrow()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.SendAsync(new GetStatisticsOperation()));
            Assert.Contains("Query string nodeTag is mandatory, but wasn't specified", e.Message);

            e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.ForNode("A").SendAsync(new GetStatisticsOperation()));
            Assert.Contains("Query string shardNumber is mandatory, but wasn't specified", e.Message);

            e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.SendAsync(new GetDetailedStatisticsOperation()));
            Assert.Contains("Query string nodeTag is mandatory, but wasn't specified", e.Message);

            e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.ForNode("A").SendAsync(new GetDetailedStatisticsOperation()));
            Assert.Contains("Query string shardNumber is mandatory, but wasn't specified", e.Message);
        }
    }
}
