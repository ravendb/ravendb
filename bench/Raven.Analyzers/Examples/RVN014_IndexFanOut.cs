// Expected diagnostic: RVN014 — "Index Map fans out over a collection"
// Fan-out indexes produce multiple index entries per source document by iterating over a
// nested collection (via SelectMany in method-chain form or nested from in query syntax).
// The RavenDB server fires a WarnIndexOutputsPerDocument warning for the same reason.
//
// This is NOT always wrong — fan-out is sometimes intentional (e.g. indexing order lines).
// Verify the collection is intentionally fanned out and that its cardinality is acceptable.
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

public class RVN014_BadIndex_MethodChain : AbstractIndexCreationTask<RVN014_Order>
{
    public RVN014_BadIndex_MethodChain()
    {
        // warning RVN014: Index Map fans out via 'SelectMany'. Each source document yields
        //   one index entry per element in the collection; unbounded collections can
        //   significantly degrade indexing performance.
        Map = orders => orders
            .SelectMany(o => o.Lines)
            .Select(l => new { l.Product, l.Quantity });
    }
}

public class RVN014_BadIndex_QuerySyntax : AbstractIndexCreationTask<RVN014_Order>
{
    public RVN014_BadIndex_QuerySyntax()
    {
        // warning RVN014 (on the inner 'from'): same fan-out, query-expression form
        Map = orders => from o in orders
                        from l in o.Lines
                        select new { l.Product, l.Quantity };
    }
}

public class RVN014_GoodIndex : AbstractIndexCreationTask<RVN014_Order>
{
    public RVN014_GoodIndex()
    {
        // No fan-out: one index entry per document
        Map = orders => from o in orders
                        select new { o.CustomerId, o.Status, LineCount = o.Lines.Count };
    }
}

public class RVN014_Order
{
    public string Id { get; set; } = default!;
    public string CustomerId { get; set; } = default!;
    public string Status { get; set; } = default!;
    public List<RVN014_Line> Lines { get; set; } = [];
}

public class RVN014_Line { public string Product { get; set; } = default!; public int Quantity { get; set; } }
