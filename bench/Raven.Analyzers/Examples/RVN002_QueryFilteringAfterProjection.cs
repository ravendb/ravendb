// Expected diagnostic: RVN002 — "RavenDB query operator after projection"
// The .Where call is placed AFTER .ProjectInto. The analyzer fires on .Where
// because projection changes the element type; filtering must come before projection.
//
// Fix: move .Where (and any other filtering/ordering) before .ProjectInto.
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN002_QueryFilteringAfterProjection
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN002: 'Where' is called after a projection
        var q = session.Query<RVN002_Order>()
            .ProjectInto<RVN002_OrderView>()
            .Where(x => x.Name == "test");
    }

    public static async Task BadExample(IAsyncDocumentSession session)
    {
        // warning RVN002: 'Where' is called after a projection
        var list = await session.Query<RVN002_Order>()
            .ProjectInto<RVN002_OrderView>()
            .Where(x => x.Name == "test")
            .ToListAsync();   // also trips RVN013 (unbounded) — intentional
    }

    public static void GoodExample(IDocumentSession session)
    {
        // Correct: filter BEFORE projection
        var q = session.Query<RVN002_Order>()
            .Where(x => x.Name == "test")
            .ProjectInto<RVN002_OrderView>();
    }
}

public class RVN002_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
public class RVN002_OrderView { public string Name { get; set; } = default!; }
