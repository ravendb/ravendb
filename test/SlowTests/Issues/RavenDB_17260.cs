using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17260 : RavenTestBase
{
    public RavenDB_17260(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task MustMoveReplacementIndexDirectoryEvenIfRunningInMemory()
    {
        using (var store = GetDocumentStore(new Options()
        {
            RunInMemory = true
        }))
        {
            var indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Foo");
            await indexToCreate.ExecuteAsync(store);

            WaitForIndexing(store);

            indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Bar");
            await indexToCreate.ExecuteAsync(store);

            WaitForIndexing(store);

            var database = await GetDatabase(store.Database);

            var options = database.IndexStore.GetIndex(indexToCreate.IndexName)._environment.Options as StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions;

            Assert.NotNull(options);

            // the reason we do want to move the replacement index directory even if running in memory is that we still create directories and temp file in that mode
            // if we modify the index definition once again we end up in putting index file in the same directory - both indexes are put into ReplacementOf_XXX folders

            Assert.DoesNotContain(Constants.Documents.Indexing.SideBySideIndexNamePrefix.Trim('/'), Path.GetDirectoryName(options.DataPager.FileName.FullPath));
        }
    }

    private class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
    {
        public class Result
        {
            public DateTime OrderedAt { get; set; }
            public string Product { get; set; }
            public decimal Profit { get; set; }
        }

        public Orders_ProfitByProductAndOrderedAt(string referencesCollectionName = null)
        {
            Map = orders => from order in orders
                from line in order.Lines
                select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

            Reduce = results => from r in results
                group r by new { r.OrderedAt, r.Product }
                into g
                select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

            OutputReduceToCollection = "Profits";

            PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";

            if (referencesCollectionName != null)
                PatternReferencesCollectionName = referencesCollectionName;
        }
    }
}
