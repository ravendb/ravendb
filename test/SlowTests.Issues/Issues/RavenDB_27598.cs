using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27598 : RavenTestBase
{
    public RavenDB_27598(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public int V { get; set; }
        public string CompanyId { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Projection
    {
        public int V { get; set; }
        public int X { get; set; }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void ObjectSpreadInJsMap()
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = "Items/Spread", Maps = { "map('Items', u => ({ ...u, Dyn2: u.V }))" } }));
        using (var session = store.OpenSession())
        {
            session.Store(new Item { V = 5 }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Item>("from index 'Items/Spread' where Dyn2 = 5").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);

            ids = session.Advanced.RawQuery<Item>("from index 'Items/Spread' where V = 5").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void SpreadOfLoadedDocumentTracksTheReference()
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/SpreadCompany", Maps = { "map('Items', u => ({ ...load(u.CompanyId, 'Companies'), V: u.V }))" }
        }));
        using (var session = store.OpenSession())
        {
            session.Store(new Company { Name = "acme" }, "companies/1");
            session.Store(new Item { V = 5, CompanyId = "companies/1" }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            session.Load<Company>("companies/1").Name = "bolt";
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Item>("from index 'Items/SpreadCompany' where Name = 'bolt'").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);
        }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.JavaScript)]
    public void ObjectSpreadInJsProjection()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new Item { V = 5 }, "items/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var result = session.Advanced.RawQuery<Projection>("from Items as u select { ...u, X: 1 }").Single();
            Assert.Equal(5, result.V);
            Assert.Equal(1, result.X);
        }
    }
}
