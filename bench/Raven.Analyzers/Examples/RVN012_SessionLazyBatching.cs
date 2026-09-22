// Expected diagnostic: RVN012 — "Batch independent session operations using the lazy API"
// Each eager Load or materializing Query (ToList, First, etc.) sends a separate HTTP request
// to the RavenDB server. When a method contains two or more independent operations, they can
// be registered as lazy and executed together in a single multi-get request, reducing latency.
//
// Fix: use session.Advanced.Lazily.Load<T>() and query.Lazily() to register lazily, then read
// the values. Reading the first .Value (or awaiting it in async code) dispatches every pending
// lazy operation in a single multi-get round-trip; no explicit call is required.
// Code fix available: Alt+Enter / Ctrl+. on the squiggle applies the fix automatically.
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN012_SessionLazyBatching
{
    public static void BadExample(IDocumentSession session, string userId, string orderId)
    {
        // warning RVN012: 'Load' is an eager session operation. This method contains
        //   multiple independent session operations; use session.Advanced.Lazily or
        //   query.Lazily() to batch them into a single server round-trip.
        var user  = session.Load<RVN012_User>(userId);   // round-trip 1
        var order = session.Load<RVN012_Order>(orderId); // round-trip 2
    }
    
    public static async Task BadExample(IAsyncDocumentSession session, string userId, string orderId)
    {
        // warning RVN012: 'Load' is an eager session operation. This method contains
        //   multiple independent session operations; use session.Advanced.Lazily or
        //   query.Lazily() to batch them into a single server round-trip.
        
        var user  = await session.LoadAsync<RVN012_User>(userId);   // round-trip 1
        var order = await session.LoadAsync<RVN012_Order>(orderId); // round-trip 2
    }

    public static void GoodExample(IDocumentSession session, string userId, string orderId)
    {
        // Correct: register both as lazy, then read the values. The first .Value access dispatches
        // both pending operations in a single multi-get round-trip.
        var lazyUser  = session.Advanced.Lazily.Load<RVN012_User>(userId);
        var lazyOrder = session.Advanced.Lazily.Load<RVN012_Order>(orderId);

        RVN012_User  user  = lazyUser.Value; // one round-trip dispatches the whole batch
        RVN012_Order order = lazyOrder.Value;
    }
}

public class RVN012_User { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
public class RVN012_Order { public string Id { get; set; } = default!; public decimal Total { get; set; } }
