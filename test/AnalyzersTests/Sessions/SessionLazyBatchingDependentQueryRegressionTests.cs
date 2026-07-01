using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Sessions;
using Xunit;

namespace AnalyzersTests.Sessions
{
    // Regression tests: RVN012 must apply the same independence check to query materializers that it
    // applies to Load arguments. A query whose chain references a prior materialized result cannot share
    // a multi-get batch and must not be flagged; two genuinely independent queries still must be.
    public class SessionLazyBatchingDependentQueryRegressionTests
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
        public async Task Query_Depending_On_Prior_Load_Result_Is_Not_Flagged()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
        var orders = session.Query<Order>().Where(o => o.OwnerId == user.Id).ToList();
    }
}
class User { public string Id { get; set; } }
class Order { public string Id { get; set; } public string OwnerId { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Two_Independent_Queries_Are_Still_Flagged()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string a, string b)
    {
        var x = session.Query<Order>().Where(o => o.OwnerId == a).ToList();
        var y = session.Query<Order>().Where(o => o.OwnerId == b).ToList();
    }
}
class Order { public string Id { get; set; } public string OwnerId { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Contains(diagnostics, d => d.Id == DiagnosticIds.SessionLazyBatching);
        }
    }
}
