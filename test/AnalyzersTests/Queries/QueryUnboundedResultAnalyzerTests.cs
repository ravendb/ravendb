using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Queries;
using Tests.Infrastructure;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryUnboundedResultAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
";

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryToList_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToList();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
            Assert.Equal(DiagnosticSeverity.Info, diagnostics[0].Severity);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryToArray_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToArray();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryToListAsync_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var users = await session.Query<User>().ToListAsync();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryToArrayAsync_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var users = await session.Query<User>().ToArrayAsync();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWithWhereAndNoTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Where(u => u.Active).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWithTake_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Take(10).ToList();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWithWhereThenTake_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Where(u => u.Active).Take(10).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWithSkipButNoTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Skip(5).ToList();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryFirst_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var user = session.Query<User>().First();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryCount_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var count = session.Query<User>().Count();
    }
}

class User { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task LocalListToList_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run()
    {
        var list = new List<User>();
        var users = list.Where(x => x.Active).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task MultipleUnboundedQueries_Reports_Multiple_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToList();
        var orders = session.Query<Order>().ToArray();
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d => Assert.Equal(DiagnosticIds.QueryUnboundedResult, d.Id));
        }

        private const string UsingsWithoutRavenLinq = @"
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
";

        private const string SearchIndex = @"
class User { public string Id { get; set; } public string Name { get; set; } public bool Active { get; set; } }
class Order { public string Id { get; set; } public string Company { get; set; } }

class Users_Search : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<User, Users_Search.Entry>
{
    public class Entry { public string Query { get; set; } }
    public Users_Search() { Map = users => from u in users select new { Query = u.Name }; }
}
";

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QuerySearchOfType_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<Users_Search.Entry, Users_Search>()
            .Search(x => x.Query, ""john"")
            .OfType<User>()
            .ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWhereAs_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<Users_Search.Entry, Users_Search>()
            .Where(x => x.Query == ""john"")
            .As<User>()
            .ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryGroupBySelect_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var results = session.Query<Order>()
            .GroupBy(o => o.Company)
            .Select(g => new { Company = g.Key, Count = g.Count() })
            .ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWhere_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Where(u => u.Active).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryStoredInIQueryableLocal_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        IQueryable<User> q = session.Query<User>().Where(u => u.Active);
        var users = q.ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task DocumentQuery_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Advanced.DocumentQuery<User>().WhereEquals(u => u.Active, true).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task AsyncDocumentQuery_WithoutTake_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var users = await session.Advanced.AsyncDocumentQuery<User>().WhereEquals(u => u.Active, true).ToListAsync();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task RawQuery_WithoutLimit_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var orders = session.Advanced.RawQuery<Order>(""from Orders"").ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryWithTake_ThroughOfType_EitherOrder_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var before = session.Query<Users_Search.Entry, Users_Search>().Take(10).OfType<User>().ToList();
        var after = session.Query<Users_Search.Entry, Users_Search>().OfType<User>().Take(10).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryGroupBySelectThenTake_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var results = session.Query<Order>()
            .GroupBy(o => o.Company)
            .Select(g => new { Company = g.Key, Count = g.Count() })
            .Take(10)
            .ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task DocumentQueryWithTake_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Advanced.DocumentQuery<User>().WhereEquals(u => u.Active, true).Take(10).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task RawQueryWithLimitInText_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    const string Prefix = ""from Orders "";

    void Run(IDocumentSession session, int count)
    {
        var literal = session.Advanced.RawQuery<Order>(""from Orders limit 10"").ToList();
        var verbatim = session.Advanced.RawQuery<Order>(@""
            from Orders
            where Company = 'companies/1'
            LIMIT 10"").ToList();
        var constant = session.Advanced.RawQuery<Order>(Prefix + ""limit 10"").ToList();
        var interpolated = session.Advanced.RawQuery<Order>($""from Orders limit {count}"").ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task RawQueryWithUnknownText_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string rql)
    {
        var orders = session.Advanced.RawQuery<Order>(rql).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task TimeSeriesGetGroupBy_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var results = session.TimeSeriesFor(""users/1"", ""HeartRate"")
            .Get()
            .GroupBy(e => e.Timestamp.Date)
            .Select(g => new { Day = g.Key, Max = g.Max(e => e.Value) })
            .ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task InMemoryToListAfterBoundedQuery_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().Take(5).ToList().Where(u => u.Active).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task PlainAsQueryable_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run()
    {
        var users = new List<User>().AsQueryable().Where(u => u.Active).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpression_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = (from u in session.Query<User>()
                     where u.Active
                     select u).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionWithLet_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var results = (from o in session.Query<Order>()
                       let company = Raven.Client.Documents.Queries.RavenQuery.Load<User>(o.Company)
                       select new { company.Name }).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionWithContinuation_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var results = (from o in session.Query<Order>()
                       group o by o.Company into g
                       select new { g.Key, Count = g.Count() }).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionOverLocal_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var q = session.Query<User>();
        var users = (from u in q
                     where u.Active
                     select u).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionToListAsync_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var users = await (from u in session.Query<User>()
                           where u.Active
                           select u).ToListAsync();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionWithTake_No_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = (from u in session.Query<User>()
                     where u.Active
                     select u).Take(10).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionOverPlainAsQueryable_No_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run()
    {
        var users = (from u in new List<User>().AsQueryable()
                     where u.Active
                     select u).ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionWithTakeInFromSource_No_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var ids = await (from u in session.Query<User>().Take(10)
                         select u.Id).ToListAsync();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionWithTakeInFromSource_MaterializedViaLocal_No_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        var ids = from u in session.Query<User>().Take(10) select u.Id;
        await ids.ToListAsync();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionOverLocalReassignedWithTake_No_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        IQueryable<User> q = session.Query<User>();
        q = q.Skip(20).Take(10);
        await (from u in q select u.Id).ToListAsync();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task LocalReassignedWithQueryExpressionOverTake_No_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session)
    {
        IQueryable<User> q = session.Query<User>();
        q = from u in q.Take(10) select u;
        q.ToList();
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionOverLocalReassignedWithTakeAfterMaterialization_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    async Task Run(IAsyncDocumentSession session)
    {
        IQueryable<User> q = session.Query<User>();
        await (from u in q select u.Id).ToListAsync();
        q = q.Take(10);
    }
}
" + SearchIndex;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryUnboundedResultAnalyzer>(source);

            Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryUnboundedResult, diagnostics[0].Id);
        }
    }
}
