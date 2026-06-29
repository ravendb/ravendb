using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Indexes;
using Xunit;

namespace AnalyzersTests.Indexes
{
    // Regression tests: RVN004/005 must consider a Map/AddMap defined in a user-defined base index
    // class, RVN006 must not fire on a single AddMap that sits in a loop, and RVN014 must see a
    // fan-out introduced after a query 'into' continuation.
    public class IndexInheritanceRegressionTests
    {
        private const string CommonUsings = @"
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
";

        [Fact]
        public async Task Map_Defined_In_Base_Index_Class_Is_Not_Flagged_As_Missing()
        {
            const string source = CommonUsings + @"
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
    protected OrderIndexBase()
    {
        Map = orders => from o in orders select new { o.Id };
    }
}

class MyIndex : OrderIndexBase
{
    public MyIndex()
    {
        Index(x => x.Id, FieldIndexing.Search);
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task AddMap_Defined_In_Base_Multi_Map_Class_Is_Not_Flagged_As_Missing()
        {
            const string source = CommonUsings + @"
abstract class BaseMulti : AbstractMultiMapIndexCreationTask<MyResult>
{
    protected BaseMulti()
    {
        AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
        AddMap<Employee>(employees => from e in employees select new MyResult { Name = e.FirstName });
    }
}

class MyMulti : BaseMulti
{
    public MyMulti() { }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
class Employee { public string FirstName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.MultiMapIndexMissingAddMap);
        }

        [Fact]
        public async Task Single_AddMap_Inside_A_Loop_Is_Not_Flagged_As_Reducible_To_Regular_Index()
        {
            // The single AddMap call site registers a map per iteration at runtime, so a multi-map
            // base is genuinely required and RVN006 must not suggest a regular index.
            const string source = CommonUsings + @"
class LoopedMulti : AbstractMultiMapIndexCreationTask<MyResult>
{
    public LoopedMulti()
    {
        foreach (var marker in new[] { ""a"", ""b"" })
            AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.MultiMapIndexSingleAddMap);
        }

        [Fact]
        public async Task FanOut_In_A_Query_Continuation_After_Into_Is_Detected()
        {
            const string source = CommonUsings + @"
class FanOutIndex : AbstractIndexCreationTask<Doc>
{
    public FanOutIndex()
    {
        Map = docs => from d in docs
                      group d by d.Category into g
                      from item in g
                      select new { item.Id };
    }
}

class Doc { public string Id { get; set; } public string Category { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexFanOut);
        }
    }
}
