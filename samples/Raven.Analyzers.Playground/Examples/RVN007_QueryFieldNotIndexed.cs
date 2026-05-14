// Expected diagnostic: RVN007 — "Query field not present in the index projection"
// When querying a specific index, Where/OrderBy/Search fields must match the fields
// projected by the index Map expression. A field not in the projection cannot be
// searched or sorted efficiently.
//
// Here the index maps only {Name, Status}, but the query filters on Price — which
// is not in the index map.
//
// Fix: add Price to the index Map, or remove the Price filter from this query.
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;

namespace Raven.Analyzers.Playground.Examples;

public class RVN007_OrderByNameIndex : AbstractIndexCreationTask<RVN007_Order>
{
    public RVN007_OrderByNameIndex()
    {
        Map = orders => from o in orders
                        select new { o.Name, o.Status };
    }
}

public static class RVN007_QueryFieldNotIndexed
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN007: Field 'Price' is referenced in Where but is not indexed by 'RVN007_OrderByNameIndex'
        var q = session.Query<RVN007_Order, RVN007_OrderByNameIndex>()
            .Where(o => o.Price > 100m);
    }

    public static void GoodExample(IDocumentSession session)
    {
        // Correct: filter only on fields that exist in the index
        var q = session.Query<RVN007_Order, RVN007_OrderByNameIndex>()
            .Where(o => o.Status == "Active")
            .Take(20);
    }
}

public class RVN007_Order
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Status { get; set; } = default!;
    public decimal Price { get; set; }
}
