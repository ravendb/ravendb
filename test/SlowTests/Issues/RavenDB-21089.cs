using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21089 : RavenTestBase
{
    public RavenDB_21089(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ImportedDocumentShouldNotHaveRevisions()
    {
        string file = "SlowTests.Smuggler.Data.Northwind_3.5.35168.ravendbdump";

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        await using var stream = GetType().Assembly.GetManifestResourceStream(file);
        using var store = GetDocumentStore();
        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 0
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        Assert.NotNull(stream);
        var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream, cts.Token);
        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation(), cts.Token);
        var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation(), cts.Token);
        AssertImport(stats, collectionStats);

        using (var session = store.OpenAsyncSession())
        {
            var order = await session.LoadAsync<Order>("orders/1", cts.Token);
            Assert.NotNull(order);

            var metadata = session.Advanced.GetMetadataFor(order);
            Assert.False(metadata.ContainsKey("Raven-Entity-Name"));
            Assert.False(metadata.ContainsKey("Raven-Last-Modified"));
            Assert.False(metadata.ContainsKey("Last-Modified"));

            var order1RevCount = await session.Advanced.Revisions.GetCountForAsync("orders/1");
            Assert.Equal(0, order1RevCount); // got 1
        }
    }

    [Fact]
    public async Task MinToKeep0ShouldNotCreateRevisions()
    {
        using var store = GetDocumentStore();
        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 0
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Order() { Id = "orders/1", Company = "A" });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Order() { Id = "orders/1", Company = "B" });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var order = await session.LoadAsync<Order>("orders/1");
            Assert.NotNull(order);

            var order1RevCount = await session.Advanced.Revisions.GetCountForAsync("orders/1");
            Assert.Equal(0, order1RevCount); // EnforceConfig: 0
        }
    }

    private void AssertImport(DatabaseStatistics stats, CollectionStatistics collectionStats)
    {
        Assert.Equal(1059, stats.CountOfDocuments);
        Assert.Equal(3, stats.CountOfIndexes); // there are 4 in ravendbdump, but Raven/DocumentsByEntityName is skipped
        Assert.Equal(1059, collectionStats.CountOfDocuments);
        Assert.Equal(9, collectionStats.Collections.Count);
        Assert.Equal(8, collectionStats.Collections["Categories"]);
        Assert.Equal(91, collectionStats.Collections["Companies"]);
        Assert.Equal(9, collectionStats.Collections["Employees"]);
        Assert.Equal(830, collectionStats.Collections["Orders"]);
        Assert.Equal(77, collectionStats.Collections["Products"]);
        Assert.Equal(4, collectionStats.Collections["Regions"]);
        Assert.Equal(3, collectionStats.Collections["Shippers"]);
        Assert.Equal(29, collectionStats.Collections["Suppliers"]);
        Assert.Equal(8, collectionStats.Collections["@hilo"]);
    }
}

