// Expected diagnostic: RVN002 — "ProjectInto called more than once in a query chain"
// Calling .ProjectInto a second time on the same query chain throws
// InvalidOperationException at runtime because the projection is already registered.
//
// Fix: remove the duplicate .ProjectInto, or restructure into two separate queries.
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN002_DoubleProjectInto
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN002: ProjectInto is called more than once on the same query chain
        var q = session.Query<RVN002_Order>()
            .ProjectInto<RVN002_OrderView>()
            .ProjectInto<RVN002_OrderSummary>();
    }

    public static void GoodExample(IDocumentSession session)
    {
        // Correct: project into only one shape
        var q = session.Query<RVN002_Order>()
            .ProjectInto<RVN002_OrderSummary>();
    }
}

public class RVN002_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
public class RVN002_OrderView { public string Name { get; set; } = default!; }
public class RVN002_OrderSummary { public string Id { get; set; } = default!; }
