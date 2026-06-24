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
        public async Task Nested_Member_Access_Checks_First_Hop_Only()
        {
            // x.Address.City → checks "Address" (first hop); if Address is indexed, no diagnostic
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
