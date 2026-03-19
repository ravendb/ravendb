using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

/// <summary>
/// RavenDB-26098: Corax facet queries with a WHERE clause use the indexed path
/// (HashSet intersection). Verifies correctness and parity with Lucene.
/// </summary>
public class RavenDB_26098 : RavenTestBase
{
    public RavenDB_26098(ITestOutputHelper output) : base(output)
    {
    }

    private class Product
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public double Price { get; set; }
        public int Stock { get; set; }
        public DateTime OrderedAt { get; set; }
        public double DurationMs { get; set; }
        public int Failed { get; set; }
    }

    private class ProductIndex : AbstractIndexCreationTask<Product>
    {
        public override string IndexName => "Products/ByCategory";

        public ProductIndex()
        {
            Map = products => from p in products
                              select new { p.Category, p.Price, p.Stock, p.OrderedAt, p.DurationMs, p.Failed };
        }
    }

    /// <summary>
    /// Range facets with WHERE + aggregations (sum/avg/min/max) must go through the
    /// scanning path and produce correct per-range aggregated values.
    /// Exercises the HandleRangeFacetsPerDocument optimization (read field once per doc).
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void RangeFacetsWithWhereClauseCanApplyAggregations(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new ProductIndex());

        var start = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        using (var session = store.OpenSession())
        {
            session.Store(new Product { Category = "Electronics", OrderedAt = start.AddDays(1), DurationMs = 10, Failed = 0 });
            session.Store(new Product { Category = "Electronics", OrderedAt = start.AddDays(2), DurationMs = 30, Failed = 1 });
            session.Store(new Product { Category = "Electronics", OrderedAt = start.AddDays(9), DurationMs = 40, Failed = 1 });
            session.Store(new Product { Category = "Electronics", OrderedAt = start.AddDays(15), DurationMs = 80, Failed = 0 });
            session.Store(new Product { Category = "Books", OrderedAt = start.AddDays(2), DurationMs = 999, Failed = 1 });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Query<Product, ProductIndex>()
                .Where(p => p.Category == "Electronics")
                .AggregateBy(facet => facet.ByRanges(
                        p => p.OrderedAt >= start && p.OrderedAt < start.AddDays(7),
                        p => p.OrderedAt >= start.AddDays(7) && p.OrderedAt < start.AddDays(14),
                        p => p.OrderedAt >= start.AddDays(14) && p.OrderedAt < start.AddDays(21))
                    .AverageOn(p => p.DurationMs)
                    .MaxOn(p => p.DurationMs)
                    .MinOn(p => p.DurationMs)
                    .SumOn(p => p.Failed))
                .Execute();

            var values = results[nameof(Product.OrderedAt)].Values;

            AssertFacetValue(values, nameof(Product.DurationMs), 2, average: 20, min: 10, max: 30, rangeFragments: new[] { start.ToString("yyyy-MM-dd"),
start.AddDays(7).ToString("yyyy-MM-dd") });
            AssertFacetValue(values, nameof(Product.Failed), 2, sum: 1, rangeFragments: new[] { start.ToString("yyyy-MM-dd"), start.AddDays(7).ToString("yyyy-MM-dd") });

            AssertFacetValue(values, nameof(Product.DurationMs), 1, average: 40, min: 40, max: 40, rangeFragments: new[] { start.AddDays(7).ToString("yyyy-MM-dd"),
start.AddDays(14).ToString("yyyy-MM-dd") });
            AssertFacetValue(values, nameof(Product.Failed), 1, sum: 1, rangeFragments: new[] { start.AddDays(7).ToString("yyyy-MM-dd"), start.AddDays(14).ToString("yyyy-MM-dd") });

            AssertFacetValue(values, nameof(Product.DurationMs), 1, average: 80, min: 80, max: 80, rangeFragments: new[] { start.AddDays(14).ToString("yyyy-MM-dd"),
start.AddDays(21).ToString("yyyy-MM-dd") });
            AssertFacetValue(values, nameof(Product.Failed), 1, sum: 0, rangeFragments: new[] { start.AddDays(14).ToString("yyyy-MM-dd"), start.AddDays(21).ToString("yyyy-MM-dd") });
        }
    }

    /// <summary>
    /// Term facets with WHERE + aggregations must go through the scanning path
    /// and produce correct per-term aggregated values including NULL_VALUE.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithWhereClauseCanApplyAggregations(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new ProductIndex());

        using (var session = store.OpenSession())
        {
            session.Store(new Product { Category = "Electronics", Price = 150, DurationMs = 10, Failed = 0 });
            session.Store(new Product { Category = "Electronics", Price = 250, DurationMs = 30, Failed = 1 });
            session.Store(new Product { Category = "Books", Price = 180, DurationMs = 40, Failed = 1 });
            session.Store(new Product { Category = "Books", Price = 50, DurationMs = 999, Failed = 1 });
            session.Store(new Product { Category = null, Price = 170, DurationMs = 20, Failed = 0 });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Query<Product, ProductIndex>()
                .Where(p => p.Price >= 100)
                .AggregateBy(facet => facet.ByField(p => p.Category)
                    .AverageOn(p => p.DurationMs)
                    .MaxOn(p => p.DurationMs)
                    .MinOn(p => p.DurationMs)
                    .SumOn(p => p.Failed))
                .Execute();

            var values = results[nameof(Product.Category)].Values;

            AssertFacetValue(values, nameof(Product.DurationMs), 2, average: 20, min: 10, max: 30, range: "electronics");
            AssertFacetValue(values, nameof(Product.Failed), 2, sum: 1, range: "electronics");

            AssertFacetValue(values, nameof(Product.DurationMs), 1, average: 40, min: 40, max: 40, range: "books");
            AssertFacetValue(values, nameof(Product.Failed), 1, sum: 1, range: "books");

            AssertFacetValue(values, nameof(Product.DurationMs), 1, average: 20, min: 20, max: 20, range: "NULL_VALUE");
            AssertFacetValue(values, nameof(Product.Failed), 1, sum: 0, range: "NULL_VALUE");
        }
    }

    private static void AssertFacetValue(List<FacetValue> values, string name, int expectedCount,
        double? average = null, double? min = null, double? max = null, double? sum = null,
        string range = null, string[] rangeFragments = null)
    {
        var matching = values.Where(v => v.Name == name);

        if (range != null)
            matching = matching.Where(v => v.Range == range);

        if (rangeFragments != null)
            matching = matching.Where(v => rangeFragments.All(f => v.Range.Contains(f)));

        var value = matching.Single();
        Assert.Equal(expectedCount, value.Count);

        if (average.HasValue)
            Assert.Equal(average.Value, value.Average.Value, 1);
        if (min.HasValue)
            Assert.Equal(min.Value, value.Min.Value, 1);
        if (max.HasValue)
            Assert.Equal(max.Value, value.Max.Value, 1);
        if (sum.HasValue)
            Assert.Equal(sum.Value, value.Sum.Value, 1);
    }
}
