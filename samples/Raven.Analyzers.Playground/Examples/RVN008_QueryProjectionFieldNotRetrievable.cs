// Expected diagnostic: RVN008 — "Projected field not retrievable under the applied ProjectionBehavior"
// RavenDB projections retrieve each field from the stored index entry or fall back to the source
// document. If a field is absent from both, the projected value will be null/missing at runtime.
// Under ProjectionBehavior.FromIndex the document fallback is disabled, so unstored fields are
// silently empty — or a hard error with FromIndexOrThrow.
//
// Here the index maps only {Name} and does not store it. The DTO has a field "Score" that
// exists neither in the index nor on the source document.
//
// Fix: either store the field in the index (Store(x => x.Score, FieldStorage.Yes)), remove
// "Score" from the DTO, or switch to ProjectionBehavior.Default to allow document fallback.
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;

namespace Raven.Analyzers.Playground.Examples;

public class RVN008_OrderNameIndex : AbstractIndexCreationTask<RVN008_Order>
{
    public RVN008_OrderNameIndex()
    {
        Map = orders => from o in orders
                        select new { o.Name };
    }
}

public static class RVN008_QueryProjectionFieldNotRetrievable
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN008: Field 'Score' in projection is not retrievable from index
        //   'RVN008_OrderNameIndex' (not stored) and is not a member of source document
        //   'RVN008_Order'. The projected value will be missing at runtime under
        //   ProjectionBehavior.FromIndex.
        var q = session.Query<RVN008_Order, RVN008_OrderNameIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndex))
            .ProjectInto<RVN008_OrderDto>();
    }
}

public class RVN008_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
public class RVN008_OrderDto { public string Name { get; set; } = default!; public int Score { get; set; } }
