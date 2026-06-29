using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Queries;
using Xunit;

namespace AnalyzersTests.Queries
{
    // Regression tests: the chain-based query analyzers follow the receiver through a local variable
    // (so a query split across statements is analyzed correctly), and RVN010 does not flag a
    // compiler-synthesized record member.
    public class QueryChainRegressionTests
    {
        private const string CommonUsings = @"
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
";

        [Fact]
        public async Task Unbounded_Result_Not_Flagged_When_Take_Is_In_A_Prior_Statement()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc>().Take(10);
        var results = q.ToList();
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Filtering_After_Projection_Detected_Across_A_Local_Variable()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var projected = session.Query<Doc>().ProjectInto<View>();
        var filtered = projected.Where(v => v.Name == ""x"");
    }
}
class Doc { public string Id { get; set; } public string Name { get; set; } }
class View { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFilteringAfterProjection);
        }

        [Fact]
        public async Task Synthesized_Record_Equals_Is_Not_Flagged_As_Unsupported_Method()
        {
            // The record's compiler-synthesized Equals(Tag) is not a user-authored helper; RavenDB
            // handles value equality, so RVN010 must not flag it.
            const string source = CommonUsings + @"
record Tag(string Value);

class Test
{
    void Run(IDocumentSession session, Tag other)
    {
        var results = session.Query<Doc>().Where(d => d.Tag.Equals(other)).ToList();
    }
}
class Doc { public string Id { get; set; } public Tag Tag { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnsupportedMethodAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
