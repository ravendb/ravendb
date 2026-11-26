using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27820(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Order
    {
        public List<string> Lines { get; set; }
    }

    private class Orders_ByProduct : AbstractIndexCreationTask<Order, Orders_ByProduct.Result>
    {
        public class Result
        {
            public string Product { get; set; }
            public int Count { get; set; }
        }

        public Orders_ByProduct()
        {
            Map = orders => from o in orders
                from l in o.Lines
                select new Result { Product = l, Count = 1 };
            Reduce = results => from r in results
                group r by r.Product into g
                select new Result { Product = g.Key, Count = g.Sum(x => x.Count) };
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Indexes)]
    public async Task DeletedDocumentMustNotLeaveReduceResults()
    {
        using var store = GetDocumentStore();
        await new Orders_ByProduct().ExecuteAsync(store);

        // more than 64 map results per document, so each document's map entries are a large fixed size tree
        const int lines = 100;
        using (var s = store.OpenAsyncSession())
        {
            await s.StoreAsync(new Order { Lines = Enumerable.Range(0, lines).Select(i => "a/" + i).ToList() }, "orders/a");
            await s.StoreAsync(new Order { Lines = Enumerable.Range(0, lines).Select(i => "b/" + i).ToList() }, "orders/b");
            await s.SaveChangesAsync();
        }
        await Indexes.WaitForIndexingAsync(store);

        // one batch: orders/a changes one line (its header is cached first), then orders/b drops all 100 of its entries
        using (var s = store.OpenAsyncSession())
        {
            (await s.LoadAsync<Order>("orders/a")).Lines[0] = "a/changed";
            (await s.LoadAsync<Order>("orders/b")).Lines.Clear();
            await s.SaveChangesAsync();
        }
        await Indexes.WaitForIndexingAsync(store);

        using (var s = store.OpenAsyncSession())
        {
            s.Delete("orders/a");
            await s.SaveChangesAsync();
        }
        await Indexes.WaitForIndexingAsync(store);

        using (var s = store.OpenAsyncSession())
        {
            var left = await s.Query<Orders_ByProduct.Result, Orders_ByProduct>().Take(5).ToListAsync();
            Assert.True(left.Count == 0, $"deleted orders/a still has reduce results, e.g. {string.Join(", ", left.Select(x => x.Product + "=" + x.Count))}");
        }
    }
}
