using System.Collections.Immutable;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Queries;
using AnalyzersTests.Framework;
using Raven.Analyzers;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryProjectionFieldAnalyzerTests
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

        // ── ProjectInto — Default behavior ────────────────────────────────────

        [Fact]
        public async Task ProjectInto_All_Fields_Stored_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Status { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
        Store(x => x.Name, FieldStorage.Yes);
        Store(x => x.Status, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_All_Fields_On_Source_Document_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Status { get; set; } }
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
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_Field_Neither_Stored_Nor_On_Source_Reports_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Ghost", d.GetMessage());
            Assert.Contains("OrderIndex", d.GetMessage());
            Assert.Contains("Order", d.GetMessage());
            Assert.Contains("Default", d.GetMessage());
        }

        [Fact]
        public async Task ProjectInto_Mixed_Stored_And_Source_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public decimal Price { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        // Name is stored; Price is on source doc (Order.Price exists) → both OK
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_StoreAllFields_Treats_Map_Projection_As_Stored_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        StoreAllFields(FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_StoreAllFields_Field_Not_In_Map_And_Not_On_Source_Reports_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        StoreAllFields(FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Ghost", d.GetMessage());
        }

        [Fact]
        public async Task ProjectInto_StoreAllFields_No_Does_Not_Treat_Fields_As_Stored_Reports_Diagnostic()
        {
            // StoreAllFields(FieldStorage.No) stores nothing, so a projected field that is only in the
            // Map (not stored, not on the source document) is not retrievable under FromIndexOrThrow.
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        StoreAllFields(FieldStorage.No);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Name", d.GetMessage());
        }

        // ── ProjectInto — FromIndex / FromIndexOrThrow ────────────────────────

        [Fact]
        public async Task ProjectInto_FromIndex_Field_On_Source_But_Not_Stored_Reports_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public decimal Price { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
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

            // Price is on source doc but not stored → diagnostic under FromIndex
            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Price", d.GetMessage());
            Assert.Contains("FromIndex", d.GetMessage());
        }

        [Fact]
        public async Task ProjectInto_FromIndex_All_Fields_Stored_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
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

        [Fact]
        public async Task ProjectInto_FromIndexOrThrow_Field_Not_Stored_Reports_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Status { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Status", d.GetMessage());
            Assert.Contains("FromIndexOrThrow", d.GetMessage());
        }

        [Fact]
        public async Task Select_FromIndexOrThrow_Id_Always_Retrievable_No_Diagnostic()
        {
            // In a Select projection the LINQ provider rewrites the identity property (x.Id) to the
            // document-id field id(), which is always retrievable even under FromIndexOrThrow where the
            // document fallback is disabled and Id is not a stored index field. So Id must not be flagged.
            const string source = CommonUsings + @"
class Doc { public string Id { get; set; } public string Name { get; set; } }
class DocIndex : AbstractIndexCreationTask<Doc>
{
    public DocIndex()
    {
        Map = docs => from d in docs select new { d.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc, DocIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .Select(x => new { x.Id });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Select_FromIndexOrThrow_Id_Retrievable_But_Other_Field_Still_Flagged()
        {
            // Id is rewritten to id() and is retrievable, but a genuinely unstored field projected next
            // to it must still be reported — the Select Id special-case must not suppress real diagnostics.
            const string source = CommonUsings + @"
class Doc { public string Id { get; set; } public string Name { get; set; } public string Ghost { get; set; } }
class DocIndex : AbstractIndexCreationTask<Doc>
{
    public DocIndex()
    {
        Map = docs => from d in docs select new { d.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc, DocIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .Select(x => new { x.Id, x.Ghost });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            // Only Ghost is flagged; Id is excluded (rewritten to id()).
            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Ghost", d.GetMessage());
        }

        [Fact]
        public async Task Select_NamedDto_FromIndexOrThrow_Id_Retrievable_No_Diagnostic()
        {
            // The named-object Select initializer (new Dto { Key = x.Id }) routes its RHS through the
            // same CheckSelectInitializerRhs Id skip as the anonymous form: x.Id is rewritten to id()
            // and is always retrievable, so it must not be flagged.
            const string source = CommonUsings + @"
class Doc { public string Id { get; set; } public string Name { get; set; } }
class Dto { public string Key { get; set; } }
class DocIndex : AbstractIndexCreationTask<Doc>
{
    public DocIndex()
    {
        Map = docs => from d in docs select new { d.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc, DocIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .Select(x => new Dto { Key = x.Id });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_FromIndexOrThrow_Unstored_Id_Reports_Diagnostic()
        {
            // Unlike Select, ProjectInto fetches member names verbatim (it emits `select Id`, not
            // `select id()`). Under FromIndexOrThrow, an Id that the index does not store is not
            // retrievable and the query throws at runtime, so RVN008 correctly fires — Id is NOT
            // special-cased in the ProjectInto path.
            const string source = CommonUsings + @"
class Doc { public string Id { get; set; } public string Name { get; set; } }
class Dto { public string Id { get; set; } }
class DocIndex : AbstractIndexCreationTask<Doc>
{
    public DocIndex()
    {
        Map = docs => from d in docs select new { d.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Doc, DocIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Id", d.GetMessage());
            Assert.Contains("FromIndexOrThrow", d.GetMessage());
        }

        // ── Stored-field extraction: expression-bodied ctor & this/base receiver ──

        [Fact]
        public async Task ProjectInto_FromIndexOrThrow_Field_Stored_Via_ThisStores_No_Diagnostic()
        {
            // The stored-field receiver is qualified (this.StoresStrings[...]); it must still be detected.
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        this.StoresStrings[""Name""] = FieldStorage.Yes;
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_FromIndexOrThrow_Stored_Via_ExpressionBodied_Ctor_No_Diagnostic()
        {
            // StoreAllFields is declared in an expression-bodied constructor; it must still be detected.
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
    }

    public OrderIndex(bool storeAll) => StoreAllFields(FieldStorage.Yes);
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── ProjectInto — FromDocument / FromDocumentOrThrow ─────────────────

        [Fact]
        public async Task ProjectInto_FromDocument_Field_On_Source_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromDocument))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ProjectInto_FromDocumentOrThrow_Field_Not_On_Source_Reports_Diagnostic()
        {
            // Ghost is not on Order; it's only reachable from the index if stored
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(""Ghost"", FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromDocumentOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            // Ghost is stored but not on source doc; FromDocumentOrThrow won't use stored value
            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Ghost", d.GetMessage());
            Assert.Contains("FromDocumentOrThrow", d.GetMessage());
        }

        // ── ProjectInto — string-form Query ──────────────────────────────────

        [Fact]
        public async Task ProjectInto_String_Form_Index_Name_Matches_Class_Checks_Fields()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>(indexName: ""OrderIndex"").ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Ghost", d.GetMessage());
        }

        [Fact]
        public async Task ProjectInto_String_Form_Unknown_Index_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Ghost { get; set; } }
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>(indexName: ""NonExistent"").ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Select form ────────────────────────────────────────────────────────

        [Fact]
        public async Task Select_Anonymous_Default_Field_On_Source_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
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
        // Price is on source doc — OK under Default (document fallback)
        var q = session.Query<Order, OrderIndex>().Select(x => new { x.Price });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Select_Anonymous_FromIndexOrThrow_Field_Not_Stored_Reports_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .Select(x => new { x.Name, x.Price });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            // Price is on source but not stored; Name is stored → only Price diagnostic
            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Select_Named_Object_FromIndexOrThrow_Field_Not_Stored_Reports_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public decimal Price { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .Select(x => new Dto { Name = x.Name, Price = x.Price });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Chained_Select_Second_Projection_No_False_Positive()
        {
            // The second Select operates on the anonymous shape produced by the first Select, not on
            // the source document / index. 'Renamed' exists only on that intermediate type, so it
            // must NOT be flagged against Order / OrderIndex.
            const string source = CommonUsings + OrderClass + @"
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
            .Select(x => new { Renamed = x.Name })
            .Select(y => new { y.Renamed });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Bail conditions ────────────────────────────────────────────────────

        [Fact]
        public async Task Query_Without_Index_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Ghost { get; set; } }
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Index_With_CreateField_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class DynamicIndex : AbstractIndexCreationTask<Order>
{
    public DynamicIndex()
    {
        Map = orders => from o in orders
                        select new { _ = CreateField(""DynField"", o.Name, true, true) };
    }
}
class Dto { public string Ghost { get; set; } }
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, DynamicIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Store_With_Variable_Field_Bails_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        string field = ""Name"";
        Store(field, FieldStorage.Yes);
    }
}
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            // Cannot determine stored set → bail, no diagnostic
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Customize_With_Variable_Behavior_Bails_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        ProjectionBehavior behavior = ProjectionBehavior.FromIndexOrThrow;
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(behavior))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Stores_Dictionary_With_Variable_Key_Bails_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        string key = ""Name"";
        StoresStrings[key] = FieldStorage.Yes;
    }
}
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Async session ──────────────────────────────────────────────────────

        [Fact]
        public async Task Async_Session_ProjectInto_Reports_Same_Diagnostic_As_Sync()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } public string Ghost { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}
class Test
{
    void Run(IAsyncDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryProjectionFieldNotRetrievable, d.Id);
            Assert.Contains("Ghost", d.GetMessage());
        }

        // ── Stores dictionary syntax ───────────────────────────────────────────

        [Fact]
        public async Task Stores_Dictionary_Indexer_Registers_Field_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Stores[x => x.Name] = FieldStorage.Yes;
    }
}
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

        [Fact]
        public async Task StoresStrings_Dictionary_Indexer_Registers_Field_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Dto { public string Name { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        StoresStrings[""Name""] = FieldStorage.Yes;
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>()
            .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
            .ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task InternalProjectionRecord_FieldOnSourceDoc_No_Diagnostic()
        {
            // Regression: SourceMemberExtractor must include internal members of source-compiled types.
            // Without the fix, all members of 'internal class Dto' with internal properties would be
            // invisible to the extractor, producing a false-positive diagnostic for each field.
            const string source = CommonUsings + OrderClass + @"
internal class Dto { internal string Name { get; set; } internal string Status { get; set; } }
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
        var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryProjectionFieldAnalyzer>(source);

            // Name and Status are on the source document (Order) — retrievable under Default behavior
            Assert.Empty(diagnostics);
        }
    }
}
