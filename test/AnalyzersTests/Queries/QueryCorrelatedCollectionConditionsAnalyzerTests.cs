using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Queries;
using Tests.Infrastructure;
using Xunit;

namespace AnalyzersTests.Queries
{
    public class QueryCorrelatedCollectionConditionsAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
";

        private const string Model = @"
enum Role { Viewer, Contributor }
class Member { public string UserId { get; set; } public Role Role { get; set; } public int Value { get; set; } public int Other { get; set; } public List<Permission> Permissions { get; set; } }
class Permission { public string Scope { get; set; } public bool Granted { get; set; } }
class Project { public string Id { get; set; } public string Owner { get; set; } public List<Member> Members { get; set; } public Member Lead { get; set; } }
class Projects_ByMember : AbstractIndexCreationTask<Project>
{
    public Projects_ByMember()
    {
        Map = projects => from p in projects select new { p.Owner };
    }
}
";

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CorrelatedAnyInDynamicQuery_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryCorrelatedCollectionConditions, diagnostic.Id);
            Assert.Equal("Any", diagnostic.Location.SourceTree!.GetText().ToString(diagnostic.Location.SourceSpan));
            Assert.Contains("'Any' over 'x.Members'", diagnostic.GetMessage());
            Assert.Contains("fan-out", diagnostic.GetMessage());
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("y.UserId == id && y.Role == Role.Contributor")]
        [InlineData("y.UserId == id")]
        public async Task All_Reports_Unsupported_Whatever_The_Conditions(string predicate)
        {
            string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.All(y => " + predicate + @"))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryCorrelatedCollectionConditions, diagnostic.Id);
            Assert.Equal("All", diagnostic.Location.SourceTree!.GetText().ToString(diagnostic.Location.SourceSpan));
            Assert.Contains("'All' over 'x.Members' is not supported", diagnostic.GetMessage());
            Assert.Contains("NotSupportedException", diagnostic.GetMessage());
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("var p = session.Query<Project>().FirstOrDefault(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = session.Query<Project>().First(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = session.Query<Project>().Single(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = session.Query<Project>().SingleOrDefault(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = session.Query<Project>().Any(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = session.Query<Project>().Count(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = session.Query<Project>().LongCount(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = await asyncSession.Query<Project>().FirstOrDefaultAsync(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        [InlineData("var p = await asyncSession.Query<Project>().AnyAsync(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor));")]
        public async Task PredicateOverload_Reports_Diagnostic(string statement)
        {
            string source = CommonUsings + @"
using System.Threading.Tasks;
class Test
{
    async Task Run(IDocumentSession session, IAsyncDocumentSession asyncSession, string id)
    {
        " + statement + @"
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Contains("'Any' over 'x.Members'", diagnostic.GetMessage());
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("session.Query<Project>(collectionName: \"Projects\")", 1)]
        [InlineData("session.Query<Project>(null, \"Projects\")", 1)]
        [InlineData("session.Query<Project>(indexName: null, collectionName: \"Projects\")", 1)]
        [InlineData("session.Query<Project>(\"Projects/ByMember\", \"Projects\")", 0)]
        [InlineData("session.Query<Project>(indexName: \"Projects/ByMember\")", 0)]
        public async Task ExplicitCollectionName_Is_Still_Dynamic(string query, int expected)
        {
            string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = " + query + @"
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Equal(expected, diagnostics.Length);
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("y.Value >= 5 && y.Value <= 10", 0)]
        [InlineData("y.Value <= 10 && y.Value >= 5", 0)]
        [InlineData("5 <= y.Value && 10 >= y.Value", 0)]
        [InlineData("y.Value >= 5 && y.Value <= 10 && y.Role == Role.Contributor", 1)]
        [InlineData("y.Value > 5 && y.Value < 10", 1)]
        [InlineData("y.Value >= 5 && y.Value < 10", 1)]
        [InlineData("y.Value >= 5 && y.Other <= 10", 1)]
        public async Task InclusiveRange_On_One_Member_Counts_As_One_Condition(string predicate, int expected)
        {
            string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => " + predicate + @"))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Equal(expected, diagnostics.Length);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ThreeConjunctsWithOneUnrelated_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id, bool flag)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => (y.UserId == id && flag) && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Single(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task SingleCondition_Is_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ConditionAndCapturedFlag_Is_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id, bool flag)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id && flag))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task OrConditions_Are_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id || y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task TwoSeparateAnyCalls_Are_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(m => m.UserId == id) && x.Members.Any(m => m.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryOverIndexType_Is_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project, Projects_ByMember>()
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryOverIndexName_Is_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>(""Projects/ByMember"")
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task NonCollectionMember_Is_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id, IEnumerable<Member> candidates)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Owner.Any(c => c == 'a' && c != 'b') && candidates.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionSyntax_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = (from x in session.Query<Project>()
                        where x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor)
                        select x).ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryCorrelatedCollectionConditions, diagnostic.Id);
            Assert.Contains("'Any' over 'x.Members'", diagnostic.GetMessage());
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionOverIndexType_Is_Silent()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = (from x in session.Query<Project, Projects_ByMember>()
                        where x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor)
                        select x).ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task NestedCollection_Reports_At_Each_Correlated_Level()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id, string scope)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id && y.Permissions.Any(p => p.Scope == scope && p.Granted)))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.Contains("'Any' over 'x.Members'", diagnostics[0].GetMessage());
            Assert.Contains("'Any' over 'y.Permissions'", diagnostics[1].GetMessage());
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CollectionReachedThroughNestedMember_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string scope)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Lead.Permissions.Any(p => p.Scope == scope && p.Granted))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Contains("'Any' over 'x.Lead.Permissions'", diagnostic.GetMessage());
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QuerySplitAcrossLocals_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var query = session.Query<Project>();
        var projects = query
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Single(diagnostics);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task AsyncSession_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Threading.Tasks;
class Test
{
    async Task Run(IAsyncDocumentSession session, string id)
    {
        var projects = await session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToListAsync();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            Assert.Single(diagnostics);
        }
        private const string UsingsWithoutRavenLinq = @"
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
";

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CorrelatedAny_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryCorrelatedCollectionConditions, diagnostic.Id);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task QueryExpressionSyntax_WithoutRavenLinqUsing_Reports_Diagnostic()
        {
            const string source = UsingsWithoutRavenLinq + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = (from x in session.Query<Project>()
                        where x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor)
                        select x).ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryCorrelatedCollectionConditions, diagnostic.Id);
            Assert.Contains("'Any' over 'x.Members'", diagnostic.GetMessage());
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CorrelatedAny_After_OfType_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string id)
    {
        var projects = session.Query<Project>()
            .OfType<Project>()
            .Where(x => x.Members.Any(y => y.UserId == id && y.Role == Role.Contributor))
            .ToList();
    }
}
" + Model;

            var diagnostics = await RavenAnalyzerTest.AnalyzeAsync<QueryCorrelatedCollectionConditionsAnalyzer>(source);

            var diagnostic = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.QueryCorrelatedCollectionConditions, diagnostic.Id);
        }
    }
}
