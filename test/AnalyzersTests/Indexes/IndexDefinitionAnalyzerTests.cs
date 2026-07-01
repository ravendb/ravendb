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

            // RVN001 fires for the assignment in Init(); RVN004 must NOT fire — the index defines a Map,
            // even though it is assigned from a helper method rather than the ctor body.
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMapAssignedOutsideCtor);
            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
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

        [Fact]
        public async Task Map_Assigned_In_Same_Class_Helper_Method_Is_Not_Flagged_As_Missing()
        {
            // The ctor delegates the Map assignment to a helper method in the same class. The index
            // does define a Map, so RVN004 must not fire. RVN001 still fires for the out-of-ctor
            // assignment — the two checks are independent.
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Configure();
    }

    private void Configure()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMapAssignedOutsideCtor);
        }

        [Fact]
        public async Task Map_Assigned_In_Base_Class_Helper_Method_Is_Not_Flagged_As_Missing()
        {
            // Base ctor delegates the Map assignment to a private helper method. The derived class is
            // clean, so analyzing it must not report RVN004 — the index defines a Map, even though it
            // is assigned from a method rather than a constructor body.
            const string source = CommonUsings + @"
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
    protected OrderIndexBase()
    {
        Configure();
    }

    private void Configure()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class MyIndex : OrderIndexBase
{
    public MyIndex() { }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task Map_Assigned_In_Transitively_Invoked_Helper_Is_Not_Flagged_As_Missing()
        {
            // ctor -> Setup() -> ApplyMap(): the Map assignment is two calls deep but still reachable
            // from the constructor, so the index defines a Map and RVN004 must not fire.
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Setup();
    }

    private void Setup()
    {
        ApplyMap();
    }

    private void ApplyMap()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task Map_Assigned_In_Uncalled_Helper_Method_Still_Reports_Missing()
        {
            // The Map is assigned only in a method the constructor never calls, so at runtime Map is
            // never set — the index genuinely has no Map and RVN004 must fire. Reachability from the
            // constructor is what distinguishes this dead helper from a real ctor-invoked one.
            const string source = CommonUsings + @"
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex() { }

    private void Configure()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task No_Map_Anywhere_In_Hierarchy_Still_Reports_Missing()
        {
            // True positive must still fire: no Map assignment exists in any constructor or method of
            // the class or its in-source base, so RVN004 is correct.
            const string source = CommonUsings + @"
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
    protected OrderIndexBase()
    {
        DoSomethingUnrelated();
    }

    private void DoSomethingUnrelated()
    {
    }
}

class MyIndex : OrderIndexBase
{
    public MyIndex() { }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task Map_Reachable_Via_Base_Helper_From_Derived_Ctor_Is_Not_Flagged_On_Derived()
        {
            // The concrete index's constructor calls a protected helper declared on the base that assigns
            // Map. The reachability walk spans the inheritance chain, so it follows the call into the base
            // method and the deployable derived index is not falsely flagged.
            const string source = CommonUsings + @"
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
    protected OrderIndexBase() { }

    protected void ConfigureMap()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class MyIndex : OrderIndexBase
{
    public MyIndex()
    {
        ConfigureMap();
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.GetMessage().Contains("MyIndex"));
        }

        [Fact]
        public async Task Map_Assigned_In_Other_Partial_Reachable_From_Ctor_Is_Not_Flagged()
        {
            // The constructor is in one partial and the Map-setting helper it calls is in another. The
            // reachability walk gathers methods and constructors across all partials, so the index is
            // recognized as defining a Map and RVN004 does not fire.
            const string source = CommonUsings + @"
partial class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Configure();
    }
}

partial class MyIndex
{
    private void Configure()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }
    }
}
