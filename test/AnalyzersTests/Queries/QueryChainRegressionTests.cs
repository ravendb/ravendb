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

        [Fact]
        public async Task OrderBy_After_Identity_Select_Is_Not_Flagged()
        {
            // A pure identity Select keeps each member's source name, so RavenDB remaps the OrderBy back
            // to the source field. RVN002 must not flag it (the confirmed false positive this fix removes).
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(o => new { o.Company, o.Total })
            .OrderBy(x => x.Total);
    }
}
class Order { public string Company { get; set; } public int Total { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task OrderBy_On_Renamed_Select_Member_Reports_Diagnostic()
        {
            // A renamed projection member (Renamed = o.Company) has no matching source field, so ordering
            // by it sorts by the alias server-side — wrong results. The Select is not a pure identity
            // projection, so RVN002 must still flag the OrderBy.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(o => new { Renamed = o.Company, o.Total })
            .OrderBy(x => x.Renamed);
    }
}
class Order { public string Company { get; set; } public int Total { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFilteringAfterProjection);
        }

        [Fact]
        public async Task Where_On_Computed_Select_Member_Reports_Diagnostic()
        {
            // A computed projection member (Full = o.First + o.Last) has no source field, so filtering by
            // it matches nothing server-side. The Select is not a pure identity projection, so RVN002
            // must still flag the Where.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(o => new { Full = o.First + o.Last })
            .Where(x => x.Full == ""John Doe"");
    }
}
class Order { public string First { get; set; } public string Last { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFilteringAfterProjection);
        }

        [Fact]
        public async Task Where_On_Captured_Variable_Projection_Reports_Diagnostic()
        {
            // The anonymous member reads a captured outer variable (captured.Name), not the lambda
            // parameter, so the projected member does not come from the source element under its source
            // name. It is not an identity projection, so RVN002 must flag the following Where. (Before the
            // fix the receiver was only required to be some identifier, so this was a missed diagnostic.)
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, Order captured)
    {
        var q = session.Query<Order>()
            .Select(o => new { captured.Name })
            .Where(x => x.Name == ""x"");
    }
}
class Order { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFilteringAfterProjection);
        }

        [Fact]
        public async Task OrderBy_After_WholeElement_Identity_Select_Is_Not_Flagged()
        {
            // o => o is a whole-element identity projection: the element is the source document unchanged,
            // so a following OrderBy still resolves to a source field. RVN002 must not flag it.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(o => o)
            .OrderBy(x => x.Total);
    }
}
class Order { public string Company { get; set; } public int Total { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Where_After_Verbatim_Parameter_Identity_Select_Is_Not_Flagged()
        {
            // A verbatim lambda parameter (@class) is a legitimate identity projection off the parameter.
            // Names must be compared decoded (ValueText) so @class.Name matches parameter @class rather
            // than being mis-flagged over the raw @-prefixed text.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(@class => new { @class.Name })
            .Where(x => x.Name == ""x"");
    }
}
class Order { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task OrderBy_After_Verbatim_WholeElement_Identity_Select_Is_Not_Flagged()
        {
            // @o => @o is the whole-element identity projection with a verbatim parameter.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(@o => @o)
            .OrderBy(x => x.Total);
    }
}
class Order { public string Company { get; set; } public int Total { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Unbounded_Result_Not_Flagged_When_Take_Applied_By_Reassignment()
        {
            // The query is bounded by a reassignment (q = q.Take(10)) inside a branch rather than in the
            // declarator. The chain walk only follows the declarator initializer, so before the
            // reassignment-aware check this was a false positive on a genuinely bounded query.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, bool paged)
    {
        var q = session.Query<Doc>();
        if (paged)
            q = q.Take(10);
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
        public async Task Unbounded_Result_Still_Flagged_When_Local_Reassigned_Without_Take()
        {
            // A query local reassigned to another still-unbounded query is genuinely unbounded — the
            // reassignment-aware suppression must not swallow this real case.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc>();
        q = q.Where(d => d.Id != null);
        var results = q.ToList();
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task Unbounded_Result_Still_Flagged_When_Take_Reassignment_Comes_After_The_Call()
        {
            // The materialization happens while the query is still unbounded; the Take is applied only
            // afterwards. The reassignment-aware suppression must consider lexical order and NOT let the
            // later Take retroactively silence the earlier, genuinely-unbounded call.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc>();
        var results = q.ToList();
        q = q.Take(10);
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [Fact]
        public async Task Unbounded_Result_Still_Flagged_When_A_Bounded_Query_Is_Only_Read_In_A_Predicate()
        {
            // A separate bounded query (recent) is captured inside the Where predicate of the unbounded
            // query. It is not on the materialized query's receiver spine, so it must not suppress the
            // outer, genuinely-unbounded ToList.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var recent = session.Query<Doc>().Take(10);
        var q = session.Query<Doc>();
        var all = q.Where(d => recent.Any(r => r.Id == d.Id)).ToList();
    }
}
class Doc { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }
    }
}
