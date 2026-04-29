using System.Collections.Immutable;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Queries;
using AnalyzersTests.Framework;
using Raven.Analyzers;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryProjectionOrderAnalyzerTests
    {
        private const string CommonUsings = @"
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
";

        [Fact]
        public async Task Where_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .Where(x => x.Id == ""1"");
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("Where", d.GetMessage());
        }

        [Fact]
        public async Task OrderBy_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .OrderBy(x => x.Id);
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
            Assert.Contains("OrderBy", d.GetMessage());
        }

        [Fact]
        public async Task GroupBy_After_Select_On_RavenQuery_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
using System;
using System.Linq.Expressions;

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Select(x => new OrderView { Id = x.Id })
            .GroupBy(x => x.Id);
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, diagnostics[0].Id);
        }

        [Fact]
        public async Task Where_After_Where_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(x => x.Status == ""active"")
            .Where(x => x.Id == ""1"");
    }

    class Order { public string Id { get; set; } public string Status { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Where_Then_ProjectInto_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(x => x.Id == ""1"")
            .OrderBy(x => x.Id)
            .ProjectInto<OrderView>();
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Where_On_Non_Raven_Queryable_No_Diagnostic()
        {
            const string source = @"
using System.Collections.Generic;
using System.Linq;

class Test
{
    void Run()
    {
        var list = new List<Order>();
        var q = list.AsQueryable()
            .Select(x => new { x.Id })
            .Where(x => x.Id == ""1"");
    }

    class Order { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Async_Where_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IAsyncDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .Where(x => x.Id == ""1"");
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
        }

        [Fact]
        public async Task Double_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .ProjectInto<OrderView2>();
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
    class OrderView2 { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.DoubleProjectInto, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
        }

        [Fact]
        public async Task Single_ProjectInto_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Where(x => x.Id == ""1"")
            .ProjectInto<OrderView>();
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Search_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .Search(x => x.Description, ""urgent"");
    }

    class Order { public string Description { get; set; } }
    class OrderView { public string Description { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("Search", d.GetMessage());
        }

        [Fact]
        public async Task Filter_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .Filter(x => x.Amount > 0);
    }

    class Order { public int Amount { get; set; } }
    class OrderView { public int Amount { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
            Assert.Contains("Filter", d.GetMessage());
        }

        [Fact]
        public async Task OrderByScore_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .OrderByScore();
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
            Assert.Contains("OrderByScore", d.GetMessage());
        }

        [Fact]
        public async Task GroupByArrayValues_After_ProjectInto_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Collections.Generic;

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .GroupByArrayValues(x => x.Tags);
    }

    class Order { public IEnumerable<string> Tags { get; set; } }
    class OrderView { public IEnumerable<string> Tags { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFilteringAfterProjection, d.Id);
            Assert.Contains("GroupByArrayValues", d.GetMessage());
        }

        [Fact]
        public async Task Search_Before_ProjectInto_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .Search(x => x.Description, ""urgent"")
            .ProjectInto<OrderView>();
    }

    class Order { public string Description { get; set; } }
    class OrderView { public string Description { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Include_After_ProjectInto_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .Include(x => x.CustomerId);
    }

    class Order { public string CustomerId { get; set; } }
    class OrderView { public string CustomerId { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Double_ProjectInto_With_Filter_Between_Reports_Both_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        // ProjectInto then Where then ProjectInto again
        var q = session.Query<Order>()
            .ProjectInto<OrderView>()
            .Where(x => x.Id == ""1"")
            .ProjectInto<OrderView2>();
    }

    class Order { public string Id { get; set; } }
    class OrderView { public string Id { get; set; } }
    class OrderView2 { public string Id { get; set; } }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionOrderAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.QueryFilteringAfterProjection);
            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.DoubleProjectInto);
        }
    }
}
