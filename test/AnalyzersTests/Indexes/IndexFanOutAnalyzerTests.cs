using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Indexes;
using Xunit;

namespace AnalyzersTests.Indexes
{
    public class IndexFanOutAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
";

        // ── Flag cases ──────────────────────────────────────────────────────────

        [Fact]
        public async Task Map_WithSelectMany_SingleArg_Reports_Diagnostic()
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
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexFanOut, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("SelectMany", d.GetMessage());
        }

        [Fact]
        public async Task Map_WithSelectMany_TwoArg_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class ClickSnapshot
{
    public IEnumerable<Click> ClickActions { get; set; }
}

class Click { public string Id { get; set; } }

class ClickIndex : AbstractIndexCreationTask<ClickSnapshot>
{
    public ClickIndex()
    {
        Map = snapshots => snapshots
            .SelectMany(x => x.ClickActions, (snapshot, x) => x);
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexFanOut, d.Id);
            Assert.Contains("SelectMany", d.GetMessage());
        }

        [Fact]
        public async Task Map_WithNestedFromClause_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class DocumentIndex : AbstractIndexCreationTask<Document>
{
    public DocumentIndex()
    {
        Map = docs => from doc in docs
                      from item in doc.Items
                      select new { item.Value };
    }
}

class Document { public IEnumerable<Item> Items { get; set; } }
class Item { public string Value { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexFanOut, d.Id);
        }

        [Fact]
        public async Task AddMap_WithSelectMany_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MultiIndex : AbstractMultiMapIndexCreationTask
{
    public MultiIndex()
    {
        AddMap<Order>(orders => orders
            .SelectMany(o => o.Lines)
            .Select(l => new { l.Product }));
        AddMap<Invoice>(invoices => invoices
            .Select(i => new { i.CustomerName }));
    }
}

class Order { public IEnumerable<Line> Lines { get; set; } }
class Line { public string Product { get; set; } }
class Invoice { public string CustomerName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.IndexFanOut, d.Id);
            Assert.Contains("SelectMany", d.GetMessage());
        }

        [Fact]
        public async Task Map_WithTwoSelectMany_Reports_TwoDiagnostics()
        {
            const string source = CommonUsings + @"
class DeepNestIndex : AbstractIndexCreationTask<Root>
{
    public DeepNestIndex()
    {
        Map = roots => roots
            .SelectMany(r => r.Children)
            .SelectMany(c => c.Items)
            .Select(i => new { i.Value });
    }
}

class Root { public IEnumerable<Child> Children { get; set; } }
class Child { public IEnumerable<Item> Items { get; set; } }
class Item { public string Value { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d => Assert.Equal(DiagnosticIds.IndexFanOut, d.Id));
        }

        // ── No-flag cases ────────────────────────────────────────────────────────

        [Fact]
        public async Task Map_WithSelectOnly_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { p.Name, p.Price };
    }
}

class Product { public string Name { get; set; } public decimal Price { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Map_WithWhereOnly_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class ActiveOrderIndex : AbstractIndexCreationTask<Order>
{
    public ActiveOrderIndex()
    {
        Map = orders => orders
            .Where(o => o.Status == ""Active"")
            .Select(o => new { o.Id, o.CustomerName });
    }
}

class Order { public string Id { get; set; } public string CustomerName { get; set; } public string Status { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task JavaScriptIndex_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Collections.Generic;

class JsIndex : AbstractJavaScriptIndexCreationTask
{
    public JsIndex()
    {
        Maps = new HashSet<string> {
            @""from order in docs.Orders
from line in order.Lines
select new { line.Product }""
        };
    }
}

class Order { public IEnumerable<Line> Lines { get; set; } }
class Line { public string Product { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task NormalIndex_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class UserIndex : AbstractIndexCreationTask<User>
{
    public UserIndex()
    {
        Map = users => users
            .Where(u => u.Active)
            .Select(u => new { u.Name, u.Email });
    }
}

class User { public string Name { get; set; } public string Email { get; set; } public bool Active { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexFanOutAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
