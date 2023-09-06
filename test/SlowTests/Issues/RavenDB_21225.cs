using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21225 : RavenTestBase
{
    public RavenDB_21225(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanStoreAndDeleteEntryWithNumericStoredField(Options options)
    {
        using var store = GetDocumentStore(options);

        new Items_Index().Execute(store);

        string id;
        using (var s = store.OpenSession())
        {
            var item = new Item("Banana", 10, 1);
            s.Store(item);
            id = s.Advanced.GetDocumentId(item);
            s.SaveChanges();
        }
        Indexes.WaitForIndexing(store);
        
        using (var s = store.OpenSession())
        {
            s.Delete(id);
            s.SaveChanges();
        }
        Indexes.WaitForIndexing(store);
        
        
        using (var s = store.OpenSession())
        {
            int count = s.Query<Items_Index>().Count();
            Assert.Equal(0, count);
        }

    }

    private record Item(string Name, double CostPerItem, int Quantity);


    private class Items_Index : AbstractIndexCreationTask<Item, Items_Index.Result>
    {
        public record Result(string Name, double Amount);
        
        public Items_Index()
        {
            Map = items =>
                from i in items
                select new { i.Name, Amount = i.Quantity * i.CostPerItem };
            
            Store(x=>x.Amount, FieldStorage.Yes);
        }
    }
}
