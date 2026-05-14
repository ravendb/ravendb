// Expected diagnostic: RVN010 — "Unsupported method call inside RavenDB query expression"
// RavenDB translates LINQ query lambdas (Where, OrderBy, Select, etc.) to RQL on the server.
// User-defined methods inside these lambdas cannot be translated and will throw at runtime.
//
// Fix: compute the value before the query, inline the logic, or call ToList() first to
// evaluate client-side.
using System.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Examples;

public class RVN010_Filters
{
    public static bool IsActiveOrder(string status) => status == "Active" || status == "Pending";
    public static int ScoreOrder(RVN010_Order o) => o.Total > 1000 ? 2 : 1;
}

public static class RVN010_QueryUnsupportedMethodCall
{
    public static void BadExample(IDocumentSession session)
    {
        // warning RVN010: Method 'IsActiveOrder' is user-defined and may not translate
        //   to server-side query semantics.
        var q = session.Query<RVN010_Order>()
            .Where(o => RVN010_Filters.IsActiveOrder(o.Status))
            .Take(20);
    }

    public static void GoodExample(IDocumentSession session)
    {
        // Correct: inline the predicate logic directly in the lambda
        var q = session.Query<RVN010_Order>()
            .Where(o => o.Status == "Active" || o.Status == "Pending")
            .Take(20);
    }
}

public class RVN010_Order { public string Id { get; set; } = default!; public string Status { get; set; } = default!; public decimal Total { get; set; } }
