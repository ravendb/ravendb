// Expected diagnostic: RVN001 — "Index Map or Reduce assigned outside constructor"
// The Map property must be assigned inside the constructor. An assignment in any
// other method (helper, public reconfigure, etc.) fires RVN001 because RavenDB reads
// the Map expression tree during index registration, which happens from the constructor.
//
// Fix: move the Map/Reduce assignment into the constructor body.
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

public class RVN001_BadIndex : AbstractIndexCreationTask<RVN001_Order>
{
    public RVN001_BadIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }

    public void Reconfigure()
    {
        // warning RVN001: 'Map' is assigned outside a constructor
        Map = orders => from o in orders select new { o.Name, o.Status, Extra = 1 };
    }
}

public class RVN001_GoodIndex : AbstractIndexCreationTask<RVN001_Order>
{
    public RVN001_GoodIndex()
    {
        // Correct: Map assigned only in the constructor
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}

public class RVN001_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; public string Status { get; set; } = default!; }
