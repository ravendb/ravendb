using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Json;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron.Debugging;
using Xunit;
using VoronConstants = Voron.Global.Constants;

namespace SlowTests.Issues;

public class RavenDB_27401(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Attachments)]
    public async Task SummaryStorageReportSizesAttachmentsFromStreamsTreeCounters()
    {
        // Lucene stores index files as streams
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        options.RunInMemory = false;
        using var store = GetDocumentStore(options);

        const int attachments = 10;
        var random = new Random(27401);
        for (var i = 0; i < attachments; i++)
        {
            var bytes = new byte[i == 0 ? 16 : random.Next(1, 2 * 1024 * 1024)]; // 16 bytes is stored inline
            random.NextBytes(bytes);

            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new User { Name = i.ToString() }, $"users/{i}");
            session.Advanced.Attachments.Store($"users/{i}", "a", new MemoryStream(bytes));
            await session.SaveChangesAsync();
        }

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var env = database.DocumentsStorage.Environment;
        using (var tx = env.ReadTransaction())
        {
            var summary = env.GenerateDetailedReport(tx, includeDetails: false);
            var detailed = env.GenerateDetailedReport(tx, includeDetails: true);

            var summaryTree = summary.Trees.Single(x => x.Streams != null);
            var detailedTree = detailed.Trees.Single(x => x.Streams != null);

            // the detailed report sizes each stream as one contiguous run, up to a page off
            const long tolerance = attachments * VoronConstants.Storage.PageSize;
            Assert.InRange(summaryTree.AllocatedSpaceInBytes - detailedTree.AllocatedSpaceInBytes, -tolerance, tolerance);
            Assert.InRange(summaryTree.Streams.AllocatedSpaceInBytes - detailedTree.Streams.AllocatedSpaceInBytes, -tolerance, tolerance);
            AssertNoUnaccountedSpace(summary);
            Assert.Null(detailed.UnaccountedSpaceInBytes);
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "0").ToListAsync();
        }

        foreach (var index in database.IndexStore.GetIndexes())
            AssertNoUnaccountedSpace(index.GenerateStorageReport(calculateExactSizes: false));
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Attachments)]
    public async Task SummaryStorageReportCountsInlineAttachmentsInTreePages()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false });

        for (var i = 0; i < 10; i++)
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new User { Name = i.ToString() }, $"users/{i}");
            session.Advanced.Attachments.Store($"users/{i}", "a", new MemoryStream(new byte[16 + i]));
            await session.SaveChangesAsync();
        }

        var env = (await Databases.GetDocumentDatabaseInstanceFor(store)).DocumentsStorage.Environment;
        using (var tx = env.ReadTransaction())
        {
            var summary = env.GenerateDetailedReport(tx, includeDetails: false);
            var summaryTree = summary.Trees.Single(x => x.Streams != null);
            var detailedTree = env.GenerateDetailedReport(tx, includeDetails: true).Trees.Single(x => x.Streams != null);

            // inline streams live in leaf pages, so there are no stream pages to count
            Assert.Equal(0, summaryTree.OverflowPages);
            Assert.Equal(0, summaryTree.Streams.AllocatedSpaceInBytes);
            Assert.Equal(detailedTree.AllocatedSpaceInBytes, summaryTree.AllocatedSpaceInBytes);
            AssertNoUnaccountedSpace(summary);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Indexes)]
    public async Task SummaryStorageReportSizesEveryStreamsTree()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        options.RunInMemory = false;
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (var i = 0; i < 100; i++)
                await session.StoreAsync(new User { Name = $"name-{i}" });
            await session.SaveChangesAsync();
        }

        var indexDefinition = new Users_ByNameWithSuggestions();
        await indexDefinition.ExecuteAsync(store);
        Indexes.WaitForIndexing(store);

        var index = (await Databases.GetDocumentDatabaseInstanceFor(store)).IndexStore.GetIndex(indexDefinition.IndexName);
        var summary = index.GenerateStorageReport(calculateExactSizes: false);

        // Lucene keeps the index files and each suggestions field in separate streams trees
        var streamsTrees = summary.Trees.Where(x => x.Streams != null).ToList();
        Assert.True(streamsTrees.Count >= 2, $"Expected at least 2 streams trees, got: {string.Join(", ", streamsTrees.Select(x => x.Name))}");

        foreach (var tree in streamsTrees)
        {
            Assert.Equal(tree.OverflowPages * VoronConstants.Storage.PageSize, tree.Streams.AllocatedSpaceInBytes);
            Assert.Equal(tree.Streams.AllocatedSpaceInBytes, tree.Streams.Streams.Single().AllocatedSpaceInBytes);
        }

        AssertNoUnaccountedSpace(summary);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.ExpirationRefresh)]
    public async Task StorageReportCountsNestedMultiValueTreePages()
    {
        using var store = GetDocumentStore();

        // every document shares one expiration time, so its multi-value entry grows into a nested tree
        using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < 2000; i++)
                await bulk.StoreAsync(new User(), $"users/{i}", new MetadataAsDictionary { [Constants.Documents.Metadata.Expires] = "2100-01-01T00:00:00.0000000Z" });
        }

        var env = (await Databases.GetDocumentDatabaseInstanceFor(store)).DocumentsStorage.Environment;
        using (var tx = env.ReadTransaction())
        {
            var tree = tx.ReadTree("DocumentsByExpiration");
            var report = env.GenerateDetailedReport(tx, includeDetails: false).Trees.Single(x => x.Name == "DocumentsByExpiration");

            Assert.True(report.MultiValues.PageCount > 0);
            Assert.Equal(tree.AllPages().Count * VoronConstants.Storage.PageSize, report.AllocatedSpaceInBytes);
        }
    }

    private class Users_ByNameWithSuggestions : AbstractIndexCreationTask<User>
    {
        public Users_ByNameWithSuggestions()
        {
            Map = users => from u in users select new { u.Name };
            Suggestion(x => x.Name);
        }
    }

    private static void AssertNoUnaccountedSpace(DetailedStorageReport report)
    {
        // a few pages are never itemized (RootObjects, large chunk lists)
        Assert.NotNull(report.UnaccountedSpaceInBytes);
        Assert.InRange(report.UnaccountedSpaceInBytes.Value, -16 * VoronConstants.Storage.PageSize, 16 * VoronConstants.Storage.PageSize);
    }
}
