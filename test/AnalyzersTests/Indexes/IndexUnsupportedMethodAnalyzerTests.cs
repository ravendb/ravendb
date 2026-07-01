using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Indexes;
using Xunit;

namespace AnalyzersTests.Indexes
{
    public class IndexUnsupportedMethodAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
";

        // ── Flag cases ──────────────────────────────────────────────────────────

        [Fact]
        public async Task LocalStaticMethod_In_Map_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("Normalize", d.GetMessage());
            Assert.Contains("Map", d.GetMessage());
        }

        [Fact]
        public async Task InstanceMethod_On_UserType_In_Map_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Product
{
    public string Name { get; set; }
    public string ComputeKey() => Name + ""_key"";
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Key = p.ComputeKey() };
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("ComputeKey", d.GetMessage());
        }

        [Fact]
        public async Task UserOverrideOfToString_In_Map_No_Diagnostic()
        {
            // A user override of Object.ToString() is rebound by the server onto DynamicBlittableJson,
            // so it works at deployment and must not be flagged as unsupported.
            const string source = CommonUsings + @"
class Product
{
    public string Name { get; set; }
    public override string ToString() => Name;
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Text = p.ToString() };
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserOverrideOfEqualsAndGetHashCode_In_Map_No_Diagnostic()
        {
            // Overrides of Object.Equals(object) and Object.GetHashCode() are rebound by the server too,
            // so calling them inside a Map must not be flagged.
            const string source = CommonUsings + @"
class Product
{
    public string Name { get; set; }
    public override bool Equals(object obj) => obj is Product p && p.Name == Name;
    public override int GetHashCode() => Name == null ? 0 : Name.GetHashCode();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Same = p.Equals(p), Hash = p.GetHashCode() };
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task TransitiveOverrideOfToString_In_Map_No_Diagnostic()
        {
            // An override declared through an intermediate user base class still (transitively) overrides
            // Object.ToString(), so the exemption must walk the OverriddenMethod chain to System.Object.
            const string source = CommonUsings + @"
class BaseDoc
{
    public override string ToString() => ""base"";
}

class Product : BaseDoc
{
    public string Name { get; set; }
    public override string ToString() => Name;
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Text = p.ToString() };
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethodNamedToString_NotAnObjectOverride_In_Map_Reports_Diagnostic()
        {
            // A user method named ToString but with a different signature is an overload, not an
            // override of Object.ToString(), so the Object-override exemption must not apply — it is
            // real user logic and stays flagged.
            const string source = CommonUsings + @"
class Product
{
    public string Name { get; set; }
    public string ToString(string format) => Name + format;
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Text = p.ToString(""x"") };
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("ToString", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_Reduce_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Utils { public static int Combine(int a, int b) => a + b; }

class CountIndex : AbstractIndexCreationTask<Order, CountIndex.Result>
{
    public class Result { public string Tag { get; set; } public int Count { get; set; } }

    public CountIndex()
    {
        Map = orders => from o in orders select new { o.Tag, Count = 1 };
        Reduce = results => from r in results
                            group r by r.Tag into g
                            select new { Tag = g.Key, Count = Utils.Combine(g.Sum(x => x.Count), 0) };
    }
}

class Order { public string Tag { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Combine", d.GetMessage());
            Assert.Contains("Reduce", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_AddMap_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyHelpers { public static string Slug(string s) => s; }

class MultiIndex : AbstractMultiMapIndexCreationTask
{
    public MultiIndex()
    {
        AddMap<Product>(products => from p in products
                                    select new { Name = MyHelpers.Slug(p.Name) });
        AddMap<Order>(orders => from o in orders
                                select new { Name = o.Title });
    }
}

class Product { public string Name { get; set; } }
class Order { public string Title { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Slug", d.GetMessage());
        }

        // ── No-flag cases ────────────────────────────────────────────────────────

        [Fact]
        public async Task BCL_StringMethod_In_Map_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new
                          {
                              Lower = p.Name.ToLower(),
                              Empty = string.IsNullOrEmpty(p.Name),
                              Abs = Math.Abs(p.Qty)
                          };
    }
}

class Product { public string Name { get; set; } public int Qty { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Linq_Methods_In_Map_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => orders
            .SelectMany(o => o.Lines)
            .Select(l => new { l.Product, l.Quantity });
    }
}

class Order { public IEnumerable<Line> Lines { get; set; } }
class Line { public string Product { get; set; } public int Quantity { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task InvocationOutsideLambda_In_Ctor_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyHelpers { public static string Slug(string s) => s; }

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products select new { p.Name };
        Store(MyHelpers.Slug(""Name""), Raven.Client.Documents.Indexes.FieldStorage.Yes);
    }
}

class Product { public string Name { get; set; } }
";
            // The Store() call is outside the Map lambda — should not fire RVN009.
            // Store itself may cause other issues, but not RVN009.
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task JavaScriptIndex_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using Raven.Client.Documents.Indexes;

class MyHelpers { public static string Slug(string s) => s; }

class JsIndex : AbstractJavaScriptIndexCreationTask
{
    public JsIndex()
    {
        Maps = new System.Collections.Generic.HashSet<string>
        {
            ""map('Products', p => ({ Name: p.Name }))""
        };
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Multiple_UserMethods_In_Map_Reports_Each()
        {
            const string source = CommonUsings + @"
class Helpers
{
    public static string Normalize(string s) => s;
    public static int Score(int n) => n;
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new
                          {
                              Name = Helpers.Normalize(p.Name),
                              Score = Helpers.Score(p.Rating)
                          };
    }
}

class Product { public string Name { get; set; } public int Rating { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d => Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id));
        }

        [Fact]
        public async Task UserMethod_In_ExpressionBodied_Ctor_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex() =>
        Map = products => from p in products select new { Name = MyHelpers.Normalize(p.Name) };
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Normalize", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_ThisMap_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        this.Map = products => from p in products select new { Name = MyHelpers.Normalize(p.Name) };
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Normalize", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_Map_With_AdditionalSources_No_Diagnostic()
        {
            // The index ships the helper's source to the server via AdditionalSources, so the server
            // compiles and can translate the call. A source-defined method reference is no longer a
            // reliable "cannot be translated" signal, so RVN009 must not fire on this index.
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
        AdditionalSources = new Dictionary<string, string>
        {
            { ""MyHelpers"", ""public static class MyHelpers { public static string Normalize(string s) => s; }"" }
        };
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_In_Map_With_AdditionalAssemblies_No_Diagnostic()
        {
            // AdditionalAssemblies references extra assemblies compiled with the index server-side, so a
            // helper call may be translatable. RVN009 must not fire on this index.
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
        AdditionalAssemblies = new HashSet<AdditionalAssembly>
        {
            AdditionalAssembly.FromRuntime(""System.Text.Json"")
        };
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_In_Map_With_AdditionalSources_Indexer_No_Diagnostic()
        {
            // Populating AdditionalSources via the indexer (AdditionalSources["Key"] = source) ships code
            // to the server just like the assignment/Add forms, so RVN009 must not fire.
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
        AdditionalSources[""MyHelpers""] = ""public static class MyHelpers { public static string Normalize(string s) => s; }"";
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task UserMethod_In_Map_With_Only_A_Read_Of_AdditionalSources_Reports_Diagnostic()
        {
            // A bare read / null-check of AdditionalSources ships no code, so it must NOT suppress RVN009 —
            // the genuinely non-translatable helper call in Map is still flagged.
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
    }

    public bool HasExtraSources() => AdditionalSources != null;
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Normalize", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_Map_With_Only_A_MemberAccess_Read_Of_AdditionalSources_Reports_Diagnostic()
        {
            // Reading a member of AdditionalSources (e.g. .Count) ships no code, so it must NOT suppress
            // RVN009 — only a write (assignment / indexer populate / .Add) does. The genuinely
            // non-translatable helper call in Map is still flagged.
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
        var count = AdditionalSources.Count;
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Normalize", d.GetMessage());
        }

        [Fact]
        public async Task UserMethod_In_Map_With_Unrelated_Local_Named_AdditionalSources_Reports_Diagnostic()
        {
            // An unrelated local that merely shares the name AdditionalSources is not the framework
            // property and ships nothing, so it must NOT suppress RVN009.
            const string source = CommonUsings + @"
class MyHelpers
{
    public static string Normalize(string s) => s.ToLower();
}

class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };
    }

    public int CountExtras()
    {
        int AdditionalSources = 3;
        return AdditionalSources;
    }
}

class Product { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexUnsupportedMethodAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexUnsupportedMethodCall, d.Id);
            Assert.Contains("Normalize", d.GetMessage());
        }
    }
}
