using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23336(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task TotalResultsShouldBeEqualWithAndWithoutSortingAsync(Options options)
    {
        using var store = GetDocumentStore(options);
        await store.ExecuteIndexAsync(new TestIndex());
        await using (var bulk = store.BulkInsert())
            foreach (var testDoc in Enumerable.Range(1, 20)
                         .Select(x => new TestDoc { Created = DateTime.UtcNow }))
                await bulk.StoreAsync(testDoc);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            // First query - without sorting
            var queryWithoutSorting = session
                .Advanced
                .AsyncDocumentQuery<TestIndex.Result, TestIndex>()
                .Statistics(out var statsWithoutSorting)
                .Take(10);

            var itemsWithoutSorting = await queryWithoutSorting.ToQueryable().As<TestDoc>().ToListAsync();

            var totalResultsWithoutSorting = statsWithoutSorting.TotalResults;
            var countWithoutSorting = itemsWithoutSorting.Count;

            // Second query - with sorting
            var queryWithSorting = session
                .Advanced
                .AsyncDocumentQuery<TestIndex.Result, TestIndex>()
                .Statistics(out var statsWithSorting)
                .Take(10)
                .OrderByDescending(sample => sample.Requested);

            var itemsWithSorting = await queryWithSorting.ToQueryable().As<TestDoc>().ToListAsync();

            var totalResultsWithSorting = statsWithSorting.TotalResults;
            var countWithSorting = itemsWithSorting.Count;

            Assert.Equal(totalResultsWithoutSorting, totalResultsWithSorting);
            Assert.Equal(countWithoutSorting, countWithSorting);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TotalResultsShouldBeEqualWithAndWithoutSortingSync(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new TestIndex());
        using (var bulk = store.BulkInsert())
        {
            foreach (var testDoc in Enumerable.Range(1, 20).Select(x => new TestDoc { Created = DateTime.UtcNow }))
            {
                bulk.Store(testDoc);
            }
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // First query - without sorting
            var queryWithoutSorting = session
                .Advanced
                .DocumentQuery<TestIndex.Result, TestIndex>()
                .Statistics(out var statsWithoutSorting)
                .Take(10);

            var itemsWithoutSorting = queryWithoutSorting.ToQueryable().As<TestDoc>().ToList();

            var totalResultsWithoutSorting = statsWithoutSorting.TotalResults;
            var countWithoutSorting = itemsWithoutSorting.Count;

            // Second query - with sorting
            var queryWithSorting = session
                .Advanced
                .DocumentQuery<TestIndex.Result, TestIndex>()
                .Statistics(out var statsWithSorting)
                .Take(10)
                .OrderByDescending(sample => sample.Requested);

            var itemsWithSorting = queryWithSorting.ToQueryable().As<TestDoc>().ToList();

            var totalResultsWithSorting = statsWithSorting.TotalResults;
            var countWithSorting = itemsWithSorting.Count;

            Assert.Equal(totalResultsWithoutSorting, totalResultsWithSorting);
            Assert.Equal(countWithoutSorting, countWithSorting);
        }
    }

    private class TestIndex : AbstractIndexCreationTask<TestDoc, TestIndex.Result>
    {
        public TestIndex()
        {
            Map = testDocs =>
                from testDoc in testDocs
                select new Result { Requested = testDoc.Created, };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }

        public class Result
        {
            public DateTime Requested { get; set; }
        }
    }

    private class TestDoc
    {
        public DateTime Created { get; set; }
    }
}
