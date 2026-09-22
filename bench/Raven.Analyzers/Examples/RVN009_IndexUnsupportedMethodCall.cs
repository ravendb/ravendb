// Expected diagnostic: RVN009 — "Unsupported method call inside index Map/Reduce expression"
// RavenDB compiles index Map and Reduce expressions to server-side IL. User-defined helper
// methods cannot be translated and will cause the index to fail at deployment or produce
// incorrect results.
//
// Fix: inline the helper logic directly in the lambda, or use only Raven-supported constructs
// (LINQ, BCL string/math operations, LoadDocument, etc.).
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

public class RVN009_Helpers
{
    public static string Normalize(string s) => s.ToLower().Trim();
    public static int ComputePriority(string status) => status == "Urgent" ? 1 : 0;
}

public class RVN009_BadIndex : AbstractIndexCreationTask<RVN009_Product>
{
    public RVN009_BadIndex()
    {
        // warning RVN009: Method 'Normalize' is user-defined and may not be
        //   translatable inside an index Map expression.
        Map = products => from p in products
                          select new
                          {
                              Name = RVN009_Helpers.Normalize(p.Name),
                              Priority = RVN009_Helpers.ComputePriority(p.Status)
                          };
    }
}

public class RVN009_GoodIndex : AbstractIndexCreationTask<RVN009_Product>
{
    public RVN009_GoodIndex()
    {
        // Correct: use BCL string operations directly in the lambda
        Map = products => from p in products
                          select new
                          {
                              Name = p.Name.ToLower().Trim(),
                              Priority = p.Status == "Urgent" ? 1 : 0
                          };
    }
}

public class RVN009_Product { public string Id { get; set; } = default!; public string Name { get; set; } = default!; public string Status { get; set; } = default!; }
