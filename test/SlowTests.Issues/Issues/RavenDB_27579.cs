using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27579 : RavenTestBase
{
    public RavenDB_27579(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
    [InlineData("from index 'Products/ByAttribute' select facet(Size)")]
    [InlineData("from index 'Products/ByAttribute' where Category = 'x' select facet(Size)")]
    public void FacetOnAFieldNoDocumentWroteIsEmpty(string rql)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Product { Category = "x", Attributes = new List<Attribute> { new() { Key = "Color", Value = "red" } } });
            session.Store(new Product { Category = "x", Attributes = new List<Attribute> { new() { Key = "Color", Value = "blue" } } });
            session.SaveChanges();
        }

        new Products_ByAttribute().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var result = session.Advanced.RawQuery<Product>(rql).ExecuteAggregation();
            Assert.Empty(result["Size"].Values);
        }
    }

    private class Attribute
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }

    private class Product
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public List<Attribute> Attributes { get; set; }
    }

    private class Products_ByAttribute : AbstractIndexCreationTask<Product>
    {
        public Products_ByAttribute()
        {
            Map = products => from p in products
                              select new
                              {
                                  p.Category,
                                  _ = p.Attributes.Select(a => CreateField(a.Key, a.Value))
                              };
        }
    }
}
