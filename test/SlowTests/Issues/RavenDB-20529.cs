using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20529 : RavenTestBase
{
    public RavenDB_20529(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void CanCountNullAsTermInFacet(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            new Index().Execute(store);
            session.Store(new Order(){Company = "maciej"});
            session.Store(new Order(){Company = null});
            session.SaveChanges();
            Indexes.WaitForIndexing(store);
        }
        
        using (var session = store.OpenSession())
        {
            var facet = session.Query<Order, Index>().AggregateBy(builder => builder.ByField(i => i.Company)).Execute();
            Assert.True(facet.ContainsKey(nameof(Order.Company)));
            var companyFacets = facet[nameof(Order.Company)].Values;
            Assert.Equal(2, companyFacets.Count);
            Assert.Equal(companyFacets.First(i => i.Range == "maciej").Count, 1);
            Assert.Equal(companyFacets.First(i => i.Range == "NULL_VALUE").Count, 1);
        }
    }

    private class Index : AbstractIndexCreationTask<Order>
    {
        public Index()
        {
            Map = orders => orders.Select(i => new {i.Company});
        }
    }
}
