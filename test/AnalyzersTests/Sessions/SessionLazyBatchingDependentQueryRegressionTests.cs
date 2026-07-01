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

        [Fact]
        public async Task Load_Whose_Id_Comes_From_Field_Assigned_By_Prior_Load_Is_Not_Flagged()
        {
            // The second load's id is a field the first load's result flowed into
            // (_customerId = order.CustomerId), so the two loads are data-dependent and cannot share a
            // multi-get. Before the fix, IsIndependentArg treated any field as context-provided, so both
            // were flagged — a false positive that the auto-fix could not honor.
            const string source = CommonUsings + @"
class Test
{
    private string _customerId;

    void Run(IDocumentSession session, string orderId)
    {
        var order = session.Load<Order>(orderId);
        _customerId = order.CustomerId;
        var customer = session.Load<Customer>(_customerId);
    }
}
class Order { public string Id { get; set; } public string CustomerId { get; set; } }
class Customer { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Load_Whose_Id_Comes_From_Property_Assigned_By_Prior_Load_Is_Not_Flagged()
        {
            // Same as the field case, through an auto-property.
            const string source = CommonUsings + @"
class Test
{
    private string CustomerId { get; set; }

    void Run(IDocumentSession session, string orderId)
    {
        var order = session.Load<Order>(orderId);
        CustomerId = order.CustomerId;
        var customer = session.Load<Customer>(CustomerId);
    }
}
class Order { public string Id { get; set; } public string CustomerId { get; set; } }
class Customer { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task Query_Whose_Predicate_Reads_Field_Assigned_By_Prior_Load_Is_Not_Flagged()
        {
            // A query predicate that reads a field fed by a prior load result depends on that load, so it
            // cannot be batched with a second independent query. Only the query on the field is dependent;
            // the load plus dependent query leave no batchable pair.
            const string source = CommonUsings + @"
class Test
{
    private string _ownerId;

    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
        _ownerId = user.Id;
        var orders = session.Query<Order>().Where(o => o.OwnerId == _ownerId).ToList();
    }
}
class User { public string Id { get; set; } }
class Order { public string Id { get; set; } public string OwnerId { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
