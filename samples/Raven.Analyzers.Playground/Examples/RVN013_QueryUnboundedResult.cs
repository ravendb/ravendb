// Expected diagnostic: RVN013 — "Query result is not bounded by Take()"
// RavenDB queries default to returning at most 128 documents per request (the server's
// page size). Without an explicit .Take(n), the intent is invisible and the query may
// silently fetch far more data than intended as the dataset grows.
//
// Fix: add .Take(n) before the materializing call (ToList, ToArray, etc.) to make the
// limit explicit and self-documenting.
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public static class RVN013_QueryUnboundedResult
{
    public static List<RVN013_Order> BadExample(IDocumentSession session, string status)
    {
        // warning RVN013: 'ToList' returns an unbounded result set. Add .Take(n) before
        //   ToList to limit the number of documents fetched from the server.
        return session.Query<RVN013_Order>()
            .Where(o => o.Status == status)
            .ToList();
    }

    public static async Task<List<RVN013_Order>> BadExampleAsync(IAsyncDocumentSession session, string status)
    {
        // warning RVN013: 'ToListAsync' returns an unbounded result set. Add .Take(n) before
        //   ToListAsync to limit the number of documents fetched from the server.
        return await session.Query<RVN013_Order>()
            .Where(o => o.Status == status)
            .ToListAsync();
    }

    public static List<RVN013_Order> GoodExample(IDocumentSession session, string status)
    {
        // Correct: explicit page size makes the limit visible at the call site
        return session.Query<RVN013_Order>()
            .Where(o => o.Status == status)
            .Take(50)
            .ToList();
    }
}

public class RVN013_Order { public string Id { get; set; } = default!; public string Status { get; set; } = default!; }
