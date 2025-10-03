using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22110 : RavenTestBase
{
    private Random _random;
    private const string MyIndexName = nameof(DistinctCountOnCollectionQuery);

    public RavenDB_22110(ITestOutputHelper output) : base(output)
    {
        _random = new(1337);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void DistinctOnStartsWithCollectionQueryWithPaging(bool useIndex)
    {
        using var store = GetDatabaseWithDocuments(useIndex);
        var indexName = useIndex ? MyIndexName : null;

        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        var databaseStatistics = session.Query<Order>(indexName).Where(x => x.Id.StartsWith("Order")).Select(x => x.Company).ToList().GroupBy(x => x)
            .ToDictionary(grouping => grouping.Key, grouping => grouping.Count());

        var concanatedResults = new List<string>();
        List<string> pagedResults;
        long totalResults = 0;
        long skippedResults = 0;
        int totalUniqueResults = 0;
        int pageNumber = 0;
        int pageSize = 10;


        do
        {
            pagedResults = session
                .Query<Order>(indexName)
                .Where(x => x.Id.StartsWith("Order"))
                .Statistics(out QueryStatistics stats)
                .Select(x => x.Company)
                .Distinct()
                .Skip((pageNumber * pageSize) + (int)skippedResults)
// Define how many items to return
                .Take(pageSize)
                .ToList();

            if (useIndex == false)
                Assert.Equal("collection/Orders", stats.IndexName);

            totalResults = stats.TotalResults; // Number of total matching documents (includes duplicates)
            skippedResults += stats.SkippedResults; // Number of duplicate results that were skipped
            totalUniqueResults += pagedResults.Count; // Number of unique results returned in this server call
            concanatedResults.AddRange(pagedResults);
            pageNumber++;
        } while (pagedResults.Count > 0); // Fetch next results

        Assert.Equal(databaseStatistics.Count, concanatedResults.Count);
        Assert.Equal(databaseStatistics.Select(x => x.Key).OrderBy(x => x), concanatedResults.OrderBy(x => x));

        Assert.Equal(1024, totalResults);
        Assert.Equal(databaseStatistics.Select(x => x.Value).Sum() - databaseStatistics.Count, skippedResults);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void DistinctCountOnCollectionQueryStartsWith(bool useIndex)
    {
        using var store = GetDatabaseWithDocuments(useIndex);
        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        var indexName = useIndex ? MyIndexName : null;

        var databaseStatistics = session.Query<Order>(indexName).Where(x => x.Id.StartsWith("Order")).Select(x => x.Company).ToList().GroupBy(x => x)
            .ToDictionary(grouping => grouping.Key, grouping => grouping.Count());

        var count = session.Query<Order>(indexName).Where(x => x.Id.StartsWith("Order")).Statistics(out var statistics).Select(x => x.Company).Distinct().Count();
        Assert.Equal(databaseStatistics.Count, count);
        Assert.Equal(databaseStatistics.Count, statistics.TotalResults);
        Assert.Equal(databaseStatistics.Select(x => x.Value).Sum() - databaseStatistics.Count, statistics.SkippedResults);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void DistinctCountOnCollectionQuery(bool useIndex)
    {
        using var store = GetDatabaseWithDocuments(useIndex);
        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        var indexName = useIndex ? MyIndexName : null;

        var databaseStatistics = session.Query<Order>(indexName).Select(x => x.Company).ToList().GroupBy(x => x)
            .ToDictionary(grouping => grouping.Key, grouping => grouping.Count());

        var count = session.Query<Order>(indexName).Statistics(out var statistics).Select(x => x.Company).Distinct().Count();
        Assert.Equal(databaseStatistics.Count, count);
        Assert.Equal(databaseStatistics.Count, statistics.TotalResults);
        Assert.Equal(databaseStatistics.Select(x => x.Value).Sum() - databaseStatistics.Count, statistics.SkippedResults);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void DistinctOnCollectionQueryWithPaging(bool useIndex)
    {
        using var store = GetDatabaseWithDocuments(useIndex);
        var indexName = useIndex ? MyIndexName : null;

        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        var databaseStatistics = session.Query<Order>(indexName).Select(x => x.Company).ToList().GroupBy(x => x)
            .ToDictionary(grouping => grouping.Key, grouping => grouping.Count());

        var concanatedResults = new List<string>();
        List<string> pagedResults;
        long totalResults = 0;
        long skippedResults = 0;
        int totalUniqueResults = 0;
        int pageNumber = 0;
        int pageSize = 10;


        do
        {
            pagedResults = session
                .Query<Order>(indexName)
                .Statistics(out QueryStatistics stats)
                .Select(x => x.Company)
                .Distinct()
                .Skip((pageNumber * pageSize) + (int)skippedResults)
// Define how many items to return
                .Take(pageSize)
                .ToList();
            totalResults = stats.TotalResults; // Number of total matching documents (includes duplicates)
            skippedResults += stats.SkippedResults; // Number of duplicate results that were skipped
            totalUniqueResults += pagedResults.Count; // Number of unique results returned in this server call
            concanatedResults.AddRange(pagedResults);
            pageNumber++;
        } while (pagedResults.Count > 0); // Fetch next results

        Assert.Equal(databaseStatistics.Count, concanatedResults.Count);
        Assert.Equal(databaseStatistics.Select(x => x.Key).OrderBy(x => x), concanatedResults.OrderBy(x => x));

        Assert.Equal(1024, totalResults);
        Assert.Equal(databaseStatistics.Select(x => x.Value).Sum() - databaseStatistics.Count, skippedResults);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void CollectionQueryStartsWithPagination(bool useIndex)
    {
        using var store = GetDatabaseWithDocuments(useIndex);
        var prefix = "orders/1";
        var indexName = useIndex ? MyIndexName : null;

        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        var databaseStatistics = session.Query<Order>(indexName).Where(x => x.Id.StartsWith(prefix)).ToList();
        var results = GetResultsInPagingManner(session.Query<Order>(indexName).Where(x => x.Id.StartsWith(prefix)), out var totalResults, out _);

        Assert.Equal(databaseStatistics.Count, totalResults);
        Assert.Equal(databaseStatistics.Select(x => x.Id), results.Select(x => x.Id));
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void CollectionQueryInPagination(bool useIndex)
    {
        using var store = GetDatabaseWithDocuments(useIndex);
        var prefix = "orders/1";
        var indexName = useIndex ? MyIndexName : null;

        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        var databaseStatistics = session.Query<Order>(indexName).Where(x => x.Id.StartsWith(prefix)).ToList();
        databaseStatistics.Shuffle();
        databaseStatistics = databaseStatistics.Take(128).ToList();


        var results = GetResultsInPagingManner(session.Query<Order>(indexName).Where(x => x.Id.In(databaseStatistics.Select(y => y.Id).ToArray())), out var totalResults,
            out _);

//In-memory sorting for assert purposes. In this case we do not care about order returned by underlying enumerator but correctness of whole result.
        databaseStatistics = databaseStatistics.OrderBy(x => x.Id).ToList();
        results = results.OrderBy(x => x.Id).ToList();

        Assert.Equal(databaseStatistics.Count, totalResults);
        Assert.Equal(databaseStatistics.Select(x => x.Id), results.Select(x => x.Id));
    }

    private List<TOut> GetResultsInPagingManner<TOut>(IRavenQueryable<TOut> baseQuery, out long totalResults, out long skippedResults)
    {
        var results = new List<TOut>();
        List<TOut> pagedResults;
        totalResults = 0;
        skippedResults = 0;
        int totalUniqueResults = 0;
        int pageNumber = 0;
        int pageSize = 10;

        do
        {
            pagedResults = baseQuery.Statistics(out QueryStatistics stats)
                .Skip((pageNumber * pageSize) + (int)skippedResults)
// Define how many items to return
                .Take(pageSize)
                .ToList();
            totalResults = stats.TotalResults; // Number of total matching documents (includes duplicates)
            skippedResults += stats.SkippedResults; // Number of duplicate results that were skipped
            totalUniqueResults += pagedResults.Count; // Number of unique results returned in this server call
            results.AddRange(pagedResults);
            pageNumber++;
        } while (pagedResults.Count > 0); // Fetch next results

        totalResults = totalUniqueResults;
        return results;
    }

    private IDocumentStore GetDatabaseWithDocuments(bool useIndex)
    {
        var store = GetDocumentStore();
        var stringTable = new[]
        {
            "class", "struct", "readonly", "final", "task", "select", "query", "index", "memory", "cpu", "counter", "timeseries", "attachment", "javascript", "C#",
            "test", "library", "database", "issues"
        };

        List<Order> orders = Enumerable.Range(0, 1024).Select(x => new Order() { Company = stringTable[_random.Next(stringTable.Length)], Freight = _random.Next(0, 16) })
            .ToList();

        using (var bulk = store.BulkInsert())
        {
            foreach (Order order in orders)
            {
                bulk.Store(order);
            }
        }

        if (useIndex)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        return store;
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(false)]
    public void CollectionQueryCountWithFilter(bool useIndex)
    {
        var indexName = useIndex ? MyIndexName : null;

        using var store = GetDatabaseWithDocuments(useIndex);
        using var session = store.OpenSession(new SessionOptions() { NoCaching = true, NoTracking = true });
        var result = session.Query<Order>(indexName).Filter(x => x.Freight == 5).ToList();
        var count = session.Query<Order>(indexName).Filter(x => x.Freight == 5).Count();
        Assert.Equal(result.Count, count);
    }

    private class Index : AbstractIndexCreationTask<Order>
    {
        public override string IndexName => MyIndexName;

        public Index()
        {
            Map = orders => orders.Select(x => new { Company = x.Company });
        }
    }
}
