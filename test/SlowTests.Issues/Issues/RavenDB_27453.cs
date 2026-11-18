using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27453(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Product
    {
        public string Id { get; set; }
        public string Region { get; set; }
        public int Price { get; set; }
    }

    private class Products_ByRegionAndPrice : AbstractIndexCreationTask<Product>
    {
        public Products_ByRegionAndPrice()
        {
            Map = products => from p in products select new { p.Region, p.Price };
        }
    }

    // The first OR branch can never match; with a negation applied to the OR it used to wipe out the whole result.
    private const string Where = "((Region = 'apac' and Region = 'eu') or Price between 32 and 66) and not Region = 'eu'";

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void EmptyBranchDoesNotEmptyTheOr()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        var products = Products();
        Store(store, products);

        var expected = products
            .Where(p => (p.Region == "apac" && p.Region == "eu" || p.Price is >= 32 and <= 66) && p.Region != "eu")
            .Select(p => p.Id)
            .ToHashSet();

        Assert.NotEmpty(expected);

        using var session = store.OpenSession();
        var actual = session.Advanced
            .RawQuery<Product>($"from index 'Products/ByRegionAndPrice' where {Where} limit 1024")
            .ToList()
            .Select(p => p.Id)
            .ToHashSet();

        Assert.True(expected.SetEquals(actual), $"expected {expected.Count}, got {actual.Count}");
    }

    private static List<Product> Products()
    {
        string[] regions = ["eu", "us", "apac"];

        return Enumerable.Range(0, 512)
            .Select(i => new Product { Id = $"products/{i}", Region = regions[i % regions.Length], Price = i % 100 })
            .ToList();
    }

    private void Store(IDocumentStore store, List<Product> products)
    {
        using (var bulk = store.BulkInsert())
        {
            foreach (var product in products)
                bulk.Store(product);
        }

        new Products_ByRegionAndPrice().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(2));
    }
}
