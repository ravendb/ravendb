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
    }
}
