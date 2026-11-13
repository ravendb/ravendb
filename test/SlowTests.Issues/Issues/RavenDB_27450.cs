using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27450(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Product
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public string Region { get; set; }
        public int Price { get; set; }
        public string[] Tags { get; set; }
    }

    private class Products_ByAll : AbstractIndexCreationTask<Product>
    {
        public Products_ByAll()
        {
            Map = products => from p in products select new { p.Category, p.Region, p.Price, p.Tags };
        }
    }

    // Both top-level clauses nest a group, so the entry-scan predicate must index every leaf inside them.
    private const string Where =
        "(Tags all in ('clearance', 'sale') and not (Tags all in ('sale', 'sale') and (Region = 'eu' or Category != 'electronics'))) " +
        "and (Region = 'us' and (Price > 15 and not Tags all in ('new', 'sale')))";

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void NestedGroupsAreScannedWithTheirOwnValues()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        var products = Products();
        Store(store, products);

        var expected = products
            .Where(p => p.Tags.Contains("clearance") && p.Tags.Contains("sale")
                        && (p.Tags.Contains("sale") && (p.Region == "eu" || p.Category != "electronics")) == false
                        && p.Region == "us"
                        && p.Price > 15
                        && (p.Tags.Contains("new") && p.Tags.Contains("sale")) == false)
            .Select(p => p.Id)
            .ToHashSet();

        // -1 disables every gate, 0..2 force one of them on, so the scan tail runs regardless of the cost heuristic.
        foreach (int gate in (int[])[-1, 0, 1, 2])
        {
            using var session = store.OpenSession();
            var actual = session.Advanced
                .RawQuery<Product>($"from index 'Products/ByAll' where {Where} limit 1024")
                .AddParameter("rvn_corax_entry_scan", gate)
                .ToList()
                .Select(p => p.Id)
                .ToHashSet();

            Assert.True(expected.SetEquals(actual), $"entry scan gate {gate}: expected {expected.Count}, got {actual.Count}");
        }
    }

    private static List<Product> Products()
    {
        string[] categories = ["electronics", "books", "toys", "food"];
        string[] regions = ["eu", "us", "apac"];
        string[] tags = ["sale", "new", "clearance", "bulk"];

        return Enumerable.Range(0, 1024)
            .Select(i => new Product
            {
                Id = $"products/{i}",
                Category = categories[i % categories.Length],
                Region = regions[i % regions.Length],
                Price = i % 100,
                Tags = [tags[i % tags.Length], tags[(i / 4) % tags.Length]]
            })
            .ToList();
    }

    private void Store(IDocumentStore store, List<Product> products)
    {
        using (var bulk = store.BulkInsert())
        {
            foreach (var product in products)
                bulk.Store(product);
        }

        new Products_ByAll().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(2));
    }
}
