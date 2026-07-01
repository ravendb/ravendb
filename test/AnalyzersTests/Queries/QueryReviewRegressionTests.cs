using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Queries;
using Xunit;

namespace AnalyzersTests.Queries
{
    // Regression tests for query-analyzer review fixes:
    //  - RVN007/RVN008 extract map/stored fields from a user-defined base index class, not just the leaf.
    //  - RVN007 also analyzes C# query-expression syntax (from/where/orderby), not only the fluent form.
    //  - RVN008 resolves the LAST-applied ProjectionBehavior when several Customize calls are chained.
    public class QueryReviewRegressionTests
    {
        private const string CommonUsings = @"
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
";

        private const string OrderClass = @"
class Order { public string Name { get; set; } public string Status { get; set; } public decimal Price { get; set; } }
";

        // Index whose Map lives in a user-defined base class; the leaf declares no constructor.
        private const string BaseClassIndex = @"
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
    protected OrderIndexBase()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}
class OrderIndex : OrderIndexBase { }
";

        // ── RVN007 walks the base index class for its map fields ──────────────────

        [Fact]
        public async Task Where_On_Field_Mapped_Only_In_Base_Index_Class_No_Diagnostic()
        {
            string source = CommonUsings + OrderClass + BaseClassIndex + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Name == ""test"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Where_On_Field_Absent_From_Base_Index_Map_Is_Still_Flagged()
        {
            // Guard: walking the base chain must still detect a genuinely unmapped field.
            string source = CommonUsings + OrderClass + BaseClassIndex + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Price > 100m);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFieldNotIndexed);
        }

        // ── RVN007 handles C# query-expression syntax ─────────────────────────────

        [Fact]
        public async Task Query_Expression_Where_On_Not_Indexed_Field_Is_Flagged()
        {
            string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order, OrderIndex>()
                where o.Price > 100m
                select o;
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFieldNotIndexed);
        }

        [Fact]
        public async Task Query_Expression_Where_And_OrderBy_On_Indexed_Fields_No_Diagnostic()
        {
            string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = from o in session.Query<Order, OrderIndex>()
                where o.Name == ""test""
                orderby o.Status
                select o;
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── RVN008 walks the base index class for stored fields ───────────────────

        [Fact]
        public async Task Projection_Field_Stored_Only_In_Base_Index_Is_Retrievable_No_Diagnostic()
        {
            string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
abstract class OrderIndexBase : AbstractIndexCreationTask<Order>
{
    protected OrderIndexBase()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class OrderIndex : OrderIndexBase { }
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndex))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── RVN008 uses the last-applied ProjectionBehavior ───────────────────────

        [Fact]
        public async Task Last_Applied_ProjectionBehavior_Wins_When_Multiple_Customize_Calls()
        {
            // The last-applied Projection (FromDocument) is the effective behavior at runtime, so the
            // source-only field 'Status' is retrievable. Resolving the first-applied FromIndex would
            // wrongly flag it against the (empty) stored set.
            string source = CommonUsings + OrderClass + @"
class Dto { public string Status { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndex))
            .Customize(x => x.Projection(ProjectionBehavior.FromDocument))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Last_Applied_ProjectionBehavior_FromIndex_Still_Flags_Unstored_Field()
        {
            // Guard the inverse: when the last-applied behavior is FromIndex, an unstored source field is
            // not retrievable and must be flagged — proving the resolver reads the outermost customize.
            string source = CommonUsings + OrderClass + @"
class Dto { public string Status { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromDocument))
            .Customize(x => x.Projection(ProjectionBehavior.FromIndex))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryProjectionFieldNotRetrievable);
        }
    }
}
