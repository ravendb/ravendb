using System.Collections.Immutable;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Queries;
using AnalyzersTests.Framework;
using Raven.Analyzers;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryIndexFieldAnalyzerTests
    {
        private const string CommonUsings = @"
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
";

        // Shared Order type used across all tests.
        // Name and Status are indexed by the standard variants; Price is intentionally not indexed.
        private const string OrderClass = @"
class Order { public string Name { get; set; } public string Status { get; set; } public decimal Price { get; set; } }
";

        private const string AnonymousObjectIndex = @"class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}";

        private const string NamedClassObjectInitializerIndex = @"class OrderResult { public string Name { get; set; } public string Status { get; set; } }
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new OrderResult { Name = o.Name, Status = o.Status };
    }
}";

        private const string MultiMapIndex = @"class Employee { public string Name { get; set; } public string Status { get; set; } }
class OrderIndex : AbstractMultiMapIndexCreationTask<Order>
{
    public OrderIndex()
    {
        AddMap<Order>(orders => from o in orders select new { o.Name, o.Status });
        AddMap<Employee>(employees => from e in employees select new { e.Name, e.Status });
    }
}";

        /// <summary>
        /// Different ways to define an index that projects {Name, Status} from Order.
        /// The same diagnostics must fire regardless of which variant is used.
        /// </summary>
        public static TheoryData<string> IndexMapVariants { get; } =
        [
            AnonymousObjectIndex,
            NamedClassObjectInitializerIndex,
            MultiMapIndex
        ];

        [Theory]
        [MemberData(nameof(IndexMapVariants))]
        public async Task Where_On_Indexed_Field_No_Diagnostic(string index)
        {
            string source = CommonUsings + OrderClass + index + @"
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

        [Theory]
        [MemberData(nameof(IndexMapVariants))]
        public async Task Where_On_Non_Indexed_Field_Reports_Diagnostic(string index)
        {
            string source = CommonUsings + OrderClass + index + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("Price", d.GetMessage());
            Assert.Contains("Where", d.GetMessage());
        }

        [Theory]
        [MemberData(nameof(IndexMapVariants))]
        public async Task Where_References_Same_Non_Indexed_Field_Twice_Reports_One_Diagnostic(string index)
        {
            string source = CommonUsings + OrderClass + index + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Price > 0 || x.Price < 0);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Where_On_Non_Indexed_Field_With_StoreAllFields_Reports_Diagnostic()
        {
            // StoreAllFields affects storage, not which fields the Map indexes, so it must not
            // suppress RVN007 for a field that is not part of the Map projection.
            string source = CommonUsings + OrderClass + @"
class StoreAllIndex : AbstractIndexCreationTask<Order>
{
    public StoreAllIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
        StoreAllFields(FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, StoreAllIndex>().Where(x => x.Price > 0);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Where_On_Indexed_Field_With_StoreAllFields_No_Diagnostic()
        {
            string source = CommonUsings + OrderClass + @"
class StoreAllIndex : AbstractIndexCreationTask<Order>
{
    public StoreAllIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
        StoreAllFields(FieldStorage.Yes);
    }
}
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, StoreAllIndex>().Where(x => x.Name == ""test"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Theory]
        [MemberData(nameof(IndexMapVariants))]
        public async Task OrderBy_On_Indexed_Field_No_Diagnostic(string index)
        {
            string source = CommonUsings + OrderClass + index + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().OrderBy(x => x.Name);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Theory]
        [MemberData(nameof(IndexMapVariants))]
        public async Task OrderBy_On_Non_Indexed_Field_Reports_Diagnostic(string index)
        {
            string source = CommonUsings + OrderClass + index + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().OrderBy(x => x.Price);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
            Assert.Contains("OrderBy", d.GetMessage());
        }

        [Theory]
        [MemberData(nameof(IndexMapVariants))]
        public async Task Search_On_Non_Indexed_Field_Reports_Diagnostic(string index)
        {
            string source = CommonUsings + OrderClass + index + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Search(x => x.Price, ""term"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
            Assert.Contains("Search", d.GetMessage());
        }

        [Fact]
        public async Task ThenByDescending_On_Non_Indexed_Field_Reports_Diagnostic()
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
        var q = session.Query<Order, OrderIndex>()
            .OrderBy(x => x.Name)
            .ThenByDescending(x => x.Price);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Nested_Member_Access_Is_Not_Flagged()
        {
            // A nested path (x.Address.City) maps to an index field name ambiguously, so it is
            // conservatively not analyzed: the intermediate segment x.Address is an object, not a
            // queried field, and flagging it ("Address") would be a false positive.
            const string source = CommonUsings + @"
class Address { public string City { get; set; } }
class Order { public Address Address { get; set; } }

class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Address };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Address.City == ""NYC"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Nested_Member_Access_To_Mapped_Leaf_No_False_Positive()
        {
            // The index maps the leaf (City = o.Shipping.City), so the field is "City". A query on
            // the nested path x.Shipping.City must NOT be flagged for the intermediate "Shipping".
            const string source = CommonUsings + @"
class Shipping { public string City { get; set; } }
class Order { public Shipping Shipping { get; set; } }

class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { City = o.Shipping.City };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Shipping.City == ""NYC"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Filter_After_Projection_Is_Not_Flagged()
        {
            // A Where after a Select binds to the projected shape, not the index, so its fields must
            // not be checked against the index field set (that is RVN002's concern). Price is absent
            // from the index Map but valid on the projected type — no RVN007 false positive.
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
            .Select(o => new { o.Name, o.Price })
            .Where(x => x.Price > 5);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Filter_Before_Projection_Is_Still_Flagged()
        {
            // The projection boundary must only suppress operators that come AFTER it: a Where on a
            // non-indexed field placed before the Select is still a real RVN007.
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
            .Where(x => x.Price > 5)
            .Select(o => new { o.Name });
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Collection_Field_Method_Call_Is_Still_Flagged()
        {
            // x.Tags.Contains(...) queries the collection field 'Tags' directly — Tags is a single-hop
            // field (not an intermediate object hop), so a non-indexed Tags must still be flagged. The
            // nested-path skip must not swallow a field that is the receiver of a method call.
            const string source = CommonUsings + @"
class Order { public string Name { get; set; } public System.Collections.Generic.List<string> Tags { get; set; } }

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
        var q = session.Query<Order, OrderIndex>().Where(x => x.Tags.Contains(""foo""));
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Tags", d.GetMessage());
        }

        [Fact]
        public async Task Where_With_Multiple_Fields_All_Indexed_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
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
        var q = session.Query<Order, OrderIndex>()
            .Where(x => x.Name == ""test"" && x.Status == ""active"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Where_With_One_Non_Indexed_Field_Reports_Single_Diagnostic()
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
        var q = session.Query<Order, OrderIndex>()
            .Where(x => x.Name == ""test"" && x.Status == ""active"");
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Status", d.GetMessage());
        }

        [Fact]
        public async Task String_Form_Literal_Matches_Index_Class_Reports_Diagnostic()
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
        var q = session.Query<Order>(indexName: ""OrderIndex"").Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task String_Form_Underscore_Name_Matched_Via_Slash_Convention()
        {
            const string source = CommonUsings + OrderClass + @"
class Orders_ByName : AbstractIndexCreationTask<Order>
{
    public Orders_ByName()
    {
        Map = orders => from o in orders select new { o.Name };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>(indexName: ""Orders/ByName"").Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task String_Form_Unknown_Index_Name_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>(indexName: ""NonExistentIndex"").Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task String_Form_Variable_Index_Name_No_Diagnostic()
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
        string indexName = ""OrderIndex"";
        var q = session.Query<Order>(indexName).Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Generic_Form_With_Overridden_IndexName_Still_Analyzed()
        {
            // The class is resolved from the type argument directly, so IndexName override is irrelevant
            const string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public override string IndexName => ""Custom/Index"";

    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task String_Form_With_Overridden_IndexName_Resolves_Literal_And_Reports_Diagnostic()
        {
            // The overridden IndexName literal is read from the property syntax and used as the lookup key
            const string source = CommonUsings + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public override string IndexName => ""Custom/Index"";

    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>(indexName: ""Custom/Index"").Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }

        [Fact]
        public async Task Query_Without_Index_No_Diagnostic()
        {
            const string source = CommonUsings + OrderClass + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order>().Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

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
                        select new
                        {
                            _ = CreateField(""DynField"", o.Name, true, true)
                        };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, DynamicIndex>().Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Index_Map_Is_Method_Call_Not_Lambda_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using System;
using System.Linq.Expressions;
" + OrderClass + @"
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = GetMap();
    }

    private static Expression<Func<System.Collections.Generic.IEnumerable<Order>, System.Collections.IEnumerable>> GetMap()
    {
        return orders => from o in orders select new { o.Name };
    }
}

class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        // ── Async session ─────────────────────────────────────────────────────

        [Fact]
        public async Task Async_Session_Reports_Same_Diagnostic_As_Sync()
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
    void Run(IAsyncDocumentSession session)
    {
        var q = session.Query<Order, OrderIndex>().Where(x => x.Price > 100);
    }
}";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<QueryIndexFieldAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryFieldNotIndexed, d.Id);
            Assert.Contains("Price", d.GetMessage());
        }
    }
}
