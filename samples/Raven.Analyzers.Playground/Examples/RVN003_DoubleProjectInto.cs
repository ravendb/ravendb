// Expected diagnostic: RVN003 — "ProjectInto called more than once in a query chain"
// Calling .ProjectInto a second time on the same query chain throws
// InvalidOperationException at runtime because the projection is already registered.
//
// Fix: remove the duplicate .ProjectInto, or restructure into two separate queries.
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN003_DoubleProjectInto
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN003: ProjectInto is called more than once on the same query chain
        var q = session.Query<RVN003_Order>()
            .ProjectInto<RVN003_OrderView>()
            .ProjectInto<RVN003_OrderSummary>();
    }

    public static async Task BadExample(IAsyncDocumentSession session)
    {
        // warning RVN003: ProjectInto is called more than once on the same query chain
        var list = await session.Query<RVN003_Order>()
            .ProjectInto<RVN003_OrderView>()
            .ProjectInto<RVN003_OrderSummary>()
            .ToListAsync();   // also trips RVN013 (unbounded) — intentional
    }

    public static void GoodExample(IDocumentSession session)
    {
        // Correct: project into only one shape
        var q = session.Query<RVN003_Order>()
            .ProjectInto<RVN003_OrderSummary>();
    }
}

public class RVN003_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
public class RVN003_OrderView { public string Name { get; set; } = default!; }
public class RVN003_OrderSummary { public string Id { get; set; } = default!; }
