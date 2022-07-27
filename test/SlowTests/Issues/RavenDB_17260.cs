using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
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
    public async Task DeleteIndexInternalWillNotThrowAccessDeniedWhenDeletingInMemoryReplacementIndex()
    {
        using (var store = GetDocumentStore(new Options()
        {
            RunInMemory = true
        }))
        {
            var indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Foo");
            await indexToCreate.ExecuteAsync(store);

            Indexes.WaitForIndexing(store);

            indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Bar");
            await indexToCreate.ExecuteAsync(store);

            Indexes.WaitForIndexing(store);

            store.Maintenance.Send(new StopIndexingOperation());

            indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Baz");
            await indexToCreate.ExecuteAsync(store);

            var database = await GetDatabase(store.Database);

            var index = database.IndexStore.GetIndex(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexToCreate.IndexName);
            var options = index._environment.Options as StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions;


            var replacementIndex = database.IndexStore.GetIndex(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexToCreate.IndexName);
            var replacementIndexOptions = replacementIndex._environment.Options as StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions;

            Assert.Equal(options.TempPath, replacementIndexOptions.TempPath);

            database.IndexStore.DeleteIndexInternal(replacementIndex, false);
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
