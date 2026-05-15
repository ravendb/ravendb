using System.Collections.Immutable;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Indexes;
using AnalyzersTests.Framework;
using Raven.Analyzers;
using Xunit;

namespace AnalyzersTests.Indexes
{
    public class IndexDefinitionAnalyzerTests
    {
        private const string CommonUsings = @"
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
";

        [Fact]
        public async Task Map_Assigned_In_Ctor_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Empty_Ctor_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex() { }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexMissingMapAssignment, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("MyIndex", d.GetMessage());
        }

        [Fact]
        public async Task No_Ctor_At_All_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexMissingMapAssignment, d.Id);
        }

        [Fact]
        public async Task Map_Reduce_Index_With_Map_In_Ctor_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order, Result>
{
    public MyIndex()
    {
        Map = orders => from o in orders select new Result { Name = o.Name, Count = 1 };
        Reduce = results => from r in results
                            group r by r.Name into g
                            select new Result { Name = g.Key, Count = g.Sum(x => x.Count) };
    }
}

class Order { public string Name { get; set; } }
class Result { public string Name { get; set; } public int Count { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Non_Index_Class_No_Diagnostic()
        {
            const string source = @"
class PlainClass
{
    public PlainClass() { }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Map_Assigned_In_Helper_Method_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Init();
    }

    private void Init()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            // RVN001 for the assignment in Init(), and RVN004 for no Map in the ctor itself
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMapAssignedOutsideCtor);
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task Map_Assigned_In_Public_Method_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Map = orders => from o in orders select new { o.Id };
    }

    public void Reconfigure()
    {
        Map = orders => from o in orders select new { o.Id, Extra = 1 };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            // RVN001 for reassignment in Reconfigure; no RVN004 (ctor has Map)
            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexMapAssignedOutsideCtor, d.Id);
            Assert.Contains("Map", d.GetMessage());
        }

        [Fact]
        public async Task Reduce_Assigned_In_Method_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order, Result>
{
    public MyIndex()
    {
        Map = orders => from o in orders select new Result { Name = o.Name, Count = 1 };
    }

    public void SetReduce()
    {
        Reduce = results => from r in results
                            group r by r.Name into g
                            select new Result { Name = g.Key, Count = g.Sum(x => x.Count) };
    }
}

class Order { public string Name { get; set; } }
class Result { public string Name { get; set; } public int Count { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexMapAssignedOutsideCtor, d.Id);
            Assert.Contains("Reduce", d.GetMessage());
        }

        [Fact]
        public async Task Local_Variable_Named_Map_In_Method_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Map = orders => from o in orders select new { o.Id };
    }

    public void DoSomething()
    {
        // A local variable named 'Map' — should not trigger RVN001
        var Map = new System.Collections.Generic.Dictionary<string, string>();
        Map[""key""] = ""value"";
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Expression-bodied constructor coverage ───────────────────────────────

        [Fact]
        public async Task Map_Assigned_In_ExpressionBodied_Ctor_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex() => Map = orders => from o in orders select new { o.Id };
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Map_Assigned_In_Method_ExpressionBody_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex() { }
    private void Configure() => Map = orders => from o in orders select new { o.Id };
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            // RVN001 for the out-of-ctor assignment; RVN004 because the ctor body is empty
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMapAssignedOutsideCtor);
        }

        [Fact]
        public async Task MultiMap_AddMap_In_ExpressionBodied_Ctor_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyMultiMapIndex : AbstractMultiMapIndexCreationTask
{
    public MyMultiMapIndex()
    {
        AddMap<Order>(orders => from o in orders select new { o.Id });
        AddMap<Product>(products => from p in products select new { p.Id });
    }
}

class Order { public string Id { get; set; } }
class Product { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── this.Map / base.Map qualified assignment coverage ────────────────────

        [Fact]
        public async Task ThisMap_Assigned_In_Ctor_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        this.Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ThisMap_Assigned_In_Method_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Map = orders => from o in orders select new { o.Id };
    }

    public void Reconfigure()
    {
        this.Map = orders => from o in orders select new { o.Id, Extra = 1 };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexMapAssignedOutsideCtor, d.Id);
        }
    }
}
