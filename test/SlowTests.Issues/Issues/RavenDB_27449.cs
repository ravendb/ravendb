using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27449(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Product
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public int Price { get; set; }
    }

    private class Products_ByCategoryAndPrice : AbstractIndexCreationTask<Product>
    {
        public Products_ByCategoryAndPrice()
        {
            Map = products => from p in products select new { p.Category, p.Price };
        }
    }

    // An OR of a negated clause and two groups makes the plan reuse a scratch bitmap slot that an earlier
    // merge already consumed. FillAllEntries has to reset the slot before it seeds it, or the query throws.
    private const string Query =
        "from index 'Products/ByCategoryAndPrice' " +
        "where Category != 'food' " +
        "   or (Category in ('books', 'toys') and Price >= 46) " +
        "   or (Price < 34 and not (Price > 10 and Price < 55)) " +
        "limit 1024";

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void OrOfNegationAndTwoGroupsReturnsTheWholeSet()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        var products = Products();
        Store(store, products);

        var expected = products
            .Where(p => p.Category != "food"
                        || (p.Category is "books" or "toys" && p.Price >= 46)
                        || (p.Price < 34 && (p.Price > 10 && p.Price < 55) == false))
            .Select(p => p.Id)
            .ToHashSet();

        Assert.NotEmpty(expected);

        using var session = store.OpenSession();
        var actual = session.Advanced.RawQuery<Product>(Query).ToList().Select(p => p.Id).ToHashSet();

        Assert.Equal(expected.Count, actual.Count);
        Assert.True(expected.SetEquals(actual));
    }

    private static List<Product> Products()
    {
        string[] categories = ["electronics", "books", "toys", "food"];

        return Enumerable.Range(0, 512)
            .Select(i => new Product { Id = $"products/{i}", Category = categories[i % categories.Length], Price = i % 100 })
            .ToList();
    }

    private void Store(IDocumentStore store, List<Product> products)
    {
        using (var bulk = store.BulkInsert())
        {
            foreach (var product in products)
                bulk.Store(product);
        }

        new Products_ByCategoryAndPrice().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(2));
    }
}
