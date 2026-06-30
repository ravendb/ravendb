using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Queries;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryUnsupportedMethodAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
";

        // ── Flag cases ──────────────────────────────────────────────────────────

        [Fact]
        public async Task UserMethod_In_Where_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyFilters { public static bool IsActive(string status) => status == ""Active""; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(o => MyFilters.IsActive(o.Status));
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("IsActive", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_OrderBy_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyScorer { public static int Score(Order o) => o.Amount; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .OrderBy(o => MyScorer.Score(o));
    }
}

class Order { public int Amount { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("Score", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_Select_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Mapper { public static string Map(Order o) => o.Name; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(o => new { Key = Mapper.Map(o) });
    }
}

class Order { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("Map", d.GetMessage());
        }

        [Fact]
        public async Task InstanceMethod_On_UserType_In_Where_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Order
{
    public string Status { get; set; }
    public bool IsActive() => Status == ""Active"";
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(o => o.IsActive());
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("IsActive", d.GetMessage());
        }

        // ── No-flag cases ────────────────────────────────────────────────────────

        [Fact]
        public async Task BCL_StringMethod_In_Where_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(o => o.Name.ToUpper() == ""ACTIVE"");
    }
}

class Order { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Math_In_Where_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(o => Math.Abs(o.Qty) > 0);
    }
}

class Order { public int Qty { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_On_NonRavenQueryable_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Collections.Generic;

class MyFilters { public static bool IsActive(string s) => s == ""Active""; }

class Test
{
    void Run()
    {
        var orders = new List<Order>();
        var q = orders.Where(o => MyFilters.IsActive(o.Status));
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Multiple_UserMethods_In_Where_Reports_Each()
        {
            const string source = CommonUsings + @"
class Predicates
{
    public static bool IsActive(string s) => s == ""Active"";
    public static bool IsLarge(int n) => n > 100;
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(o => Predicates.IsActive(o.Status) && Predicates.IsLarge(o.Amount));
    }
}

class Order { public string Status { get; set; } public int Amount { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d => Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id));
        }

        [Fact]
        public async Task UserMethod_Outside_Lambda_In_Chain_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyHelpers { public static string GetIndex() => ""MyIndex""; }

class Test
{
    void Run(IDocumentSession session)
    {
        // MyHelpers.GetIndex() is not inside a lambda passed to the query chain
        var q = session.Query<Order>(MyHelpers.GetIndex())
            .Where(o => o.Status == ""Active"");
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Filter (added to QueryChainLambdaMethods) ─────────────────────────

        [Fact]
        public async Task UserMethod_In_Filter_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
using Raven.Client.Documents.Queries;
class MyPredicates { public static bool IsImportant(string status) => status == ""Important""; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Filter(o => MyPredicates.IsImportant(o.Status));
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("IsImportant", d.GetMessage());
        }

        [Fact]
        public async Task BclMethod_In_Filter_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using Raven.Client.Documents.Queries;

class Test
{
    void Run(IDocumentSession session)
    {
        // string.IsNullOrEmpty is BCL (referenced assembly) — should not be flagged
        var q = session.Query<Order>()
            .Filter(o => !string.IsNullOrEmpty(o.Status));
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_Outside_Lambda_In_OrderByDistance_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using Raven.Client.Documents.Queries;
class GeoHelper { public static double GetLat() => 52.0; public static double GetLng() => 21.0; }

class Test
{
    void Run(IDocumentSession session)
    {
        // GeoHelper calls are NOT inside the field-selector lambda — should not be flagged
        var q = session.Query<Place>()
            .OrderByDistance(p => p.Location, GeoHelper.GetLat(), GeoHelper.GetLng());
    }
}

class Place { public string Location { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Query-expression (from/where/select) form ─────────────────────────────

        [Fact]
        public async Task UserMethod_In_QueryExpression_Where_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyFilters { public static bool IsActive(string status) => status == ""Active""; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order>()
                where MyFilters.IsActive(o.Status)
                select o;
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("IsActive", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_QueryExpression_Select_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Mapper { public static string Map(Order o) => o.Name; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order>()
                select Mapper.Map(o);
    }
}

class Order { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("Map", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_QueryExpression_OrderBy_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyScorer { public static int Score(Order o) => o.Amount; }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order>()
                orderby MyScorer.Score(o)
                select o;
    }
}

class Order { public int Amount { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("Score", d.GetMessage());
        }

        [Fact]
        public async Task BclMethod_In_QueryExpression_Where_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order>()
                where o.Name.ToUpper() == ""ACTIVE""
                select o;
    }
}

class Order { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_In_NonRaven_QueryExpression_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Collections.Generic;

class MyFilters { public static bool IsActive(string s) => s == ""Active""; }

class Test
{
    void Run()
    {
        var orders = new List<Order>();
        var q = from o in orders
                where MyFilters.IsActive(o.Status)
                select o;
    }
}

class Order { public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_In_EmbeddedRavenChain_Inside_QueryExpression_Reports_Once()
        {
            // A method-chain Raven query embedded in a 'select' clause is reported by the method-chain
            // path; the query-expression path must not report it a second time at the same location.
            const string source = CommonUsings + @"
class H { public static bool M(Item i) => i.On; }
class Item { public bool On { get; set; } }
class Order { }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order>()
                select session.Query<Item>().Where(i => H.M(i)).ToList();
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("M", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_NonRavenLambda_Inside_QueryExpression_Where_Reports_Once()
        {
            // A lambda passed to a non-Raven method (Enumerable.Any) inside a clause is NOT owned by the
            // method-chain path, so the query-expression path must still report the user method in it.
            const string source = CommonUsings + @"
using System.Collections.Generic;
class H { public static bool M(string s) => s.Length > 0; }
class Order { public List<string> Tags { get; set; } }

class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order>()
                where o.Tags.Any(t => H.M(t))
                select o;
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnsupportedMethodCall, d.Id);
            Assert.Contains("M", d.GetMessage());
        }

        // ── Type matching is gated on the Raven.Client namespace ───────────────────

        [Fact]
        public async Task UserMethod_On_SameNamedNonRavenQueryable_No_Diagnostic()
        {
            // A user type named IRavenQueryable that is NOT in the Raven.Client namespace must not be
            // treated as the RavenDB queryable, so its Where lambda is never analyzed.
            const string source = @"
using System;

namespace MyApp
{
    public interface IRavenQueryable<T> { IRavenQueryable<T> Where(Func<T, bool> predicate); }
    public static class Q { public static IRavenQueryable<T> Query<T>() => null!; }
    public static class MyFilters { public static bool IsActive(string s) => s == ""Active""; }
    public class Order { public string Status { get; set; } }

    public class Test
    {
        public void Run()
        {
            var q = Q.Query<Order>().Where(o => MyFilters.IsActive(o.Status));
        }
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
