using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
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

    private static void AssertNoUnaccountedSpace(DetailedStorageReport report)
    {
        // a few pages are never itemized (RootObjects, large chunk lists)
        Assert.NotNull(report.UnaccountedSpaceInBytes);
        Assert.InRange(report.UnaccountedSpaceInBytes.Value, -16 * VoronConstants.Storage.PageSize, 16 * VoronConstants.Storage.PageSize);
    }
}
