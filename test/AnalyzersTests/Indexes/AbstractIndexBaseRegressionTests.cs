using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Indexes;
using Xunit;

namespace AnalyzersTests.Indexes
{
    // Regression tests: an abstract index base is never instantiated or deployed on its own, so the
    // "missing map" checks (RVN004/RVN005) must not fire on it — its concrete subclasses supply the
    // Map/AddMap. The exemption must not, however, suppress the diagnostic for a concrete class.
    public class AbstractIndexBaseRegressionTests
    {
        private const string CommonUsings = @"
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
";

        [Fact]
        public async Task Abstract_Index_Base_Whose_Subclasses_Define_Map_Is_Not_Flagged_As_Missing()
        {
            const string source = CommonUsings + @"
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
}

class MyIndex : OrderIndexBase
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

            Assert.DoesNotContain(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }

        [Fact]
        public async Task Abstract_Multi_Map_Base_Whose_Subclasses_Add_Maps_Is_Not_Flagged_As_Missing()
        {
            const string source = CommonUsings + @"
abstract class BaseMulti : AbstractMultiMapIndexCreationTask<MyResult>
{
}

class MyMulti : BaseMulti
{
    public MyMulti()
    {
        AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
        AddMap<Employee>(employees => from e in employees select new MyResult { Name = e.FirstName });
    }
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
        public async Task Concrete_Index_With_No_Map_Is_Still_Flagged_As_Missing()
        {
            // Guard: the abstract-class exemption must not leak to a concrete, deployable index.
            const string source = CommonUsings + @"
class EmptyIndex : AbstractIndexCreationTask<Order>
{
    public EmptyIndex()
    {
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.IndexMissingMapAssignment);
        }
    }
}
