// Expected diagnostic: RVN003 — "Index Map or Reduce assigned outside constructor"
// The Map property must be assigned inside the constructor. An assignment in any
// other method (helper, public reconfigure, etc.) fires RVN003 because RavenDB reads
// the Map expression tree during index registration, which happens from the constructor.
//
// Fix: move the Map/Reduce assignment into the constructor body.
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

public class RVN003_BadIndex : AbstractIndexCreationTask<RVN003_Order>
{
    public RVN003_BadIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }

    public void Reconfigure()
    {
        // warning RVN003: 'Map' is assigned outside a constructor
        Map = orders => from o in orders select new { o.Name, o.Status, Extra = 1 };
    }
}

public class RVN003_GoodIndex : AbstractIndexCreationTask<RVN003_Order>
{
    public RVN003_GoodIndex()
    {
        // Correct: Map assigned only in the constructor
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}

public class RVN003_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; public string Status { get; set; } = default!; }
