// Expected diagnostic: RVN001 — "RavenDB query operator after projection"
// The .Where call is placed AFTER .ProjectInto. The analyzer fires on .Where
// because projection changes the element type; filtering must come before projection.
//
// Fix: move .Where (and any other filtering/ordering) before .ProjectInto.
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN001_QueryFilteringAfterProjection
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN001: 'Where' is called after a projection
        var q = session.Query<RVN001_Order>()
            .ProjectInto<RVN001_OrderView>()
            .Where(x => x.Name == "test");
    }

    public static void GoodExample(IDocumentSession session)
    {
        // Correct: filter BEFORE projection
        var q = session.Query<RVN001_Order>()
            .Where(x => x.Name == "test")
            .ProjectInto<RVN001_OrderView>();
    }
}

public class RVN001_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
public class RVN001_OrderView { public string Name { get; set; } = default!; }
