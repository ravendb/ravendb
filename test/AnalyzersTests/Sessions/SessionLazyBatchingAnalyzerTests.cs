using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Sessions;
using Xunit;

namespace AnalyzersTests.Sessions
{
    public class SessionLazyBatchingAnalyzerTests
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

        // ── Flag cases ──────────────────────────────────────────────────────────

        [Fact]
        public async Task TwoLoads_WithParamIds_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string orderId)
    {
        var user = session.Load<User>(userId);
        var order = session.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d =>
            {
                Assert.Equal(DiagnosticIds.SessionLazyBatching, d.Id);
                Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            });
        }

        [Fact]
        public async Task QueryToList_And_Load_WithParamId_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string managerId)
    {
        var users = session.Query<User>().ToList();
        var manager = session.Load<User>(managerId);
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d =>
            {
                Assert.Equal(DiagnosticIds.SessionLazyBatching, d.Id);
                Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            });
        }

        [Fact]
        public async Task TwoQueries_InSameMethod_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var activeUsers = session.Query<User>().Where(u => u.Active).ToList();
        var inactiveUsers = session.Query<User>().Where(u => !u.Active).ToList();
    }
}

class User { public string Id { get; set; } public bool Active { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d =>
            {
                Assert.Equal(DiagnosticIds.SessionLazyBatching, d.Id);
                Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            });
        }

        [Fact]
        public async Task Load_Before_Query_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
        var allUsers = session.Query<User>().ToList();
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
        }

        [Fact]
        public async Task TwoAsyncLoads_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string userId, string orderId)
    {
        var user = await session.LoadAsync<User>(userId);
        var order = await session.LoadAsync<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
        }

        [Fact]
        public async Task AsyncQuery_And_AsyncLoad_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string managerId)
    {
        var users = await session.Query<User>().ToListAsync();
        var manager = await session.LoadAsync<User>(managerId);
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
        }

        [Fact]
        public async Task Load_With_FieldId_Reports_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    private readonly string _managerId = ""managers/1"";

    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
        var manager = session.Load<User>(_managerId);
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
        }

        // ── No-flag cases ────────────────────────────────────────────────────────

        [Fact]
        public async Task SingleLoad_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId)
    {
        var user = session.Load<User>(userId);
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task SingleQuery_No_Diagnostic()
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
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task DependentLoad_ComplexExpr_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToList();
        var manager = session.Load<User>(users[0].ManagerId);
    }
}

class User { public string Id { get; set; } public string ManagerId { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // No diagnostics: query is independent, but load is dependent on query result
            // Only report when there are 2+ batchable operations
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task DependentLoad_DerivedLocal_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var users = session.Query<User>().ToList();
        var managerId = users.First().ManagerId;
        var manager = session.Load<User>(managerId);
    }
}

class User { public string Id { get; set; } public string ManagerId { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // No diagnostics: managerId init is complex, so treated as dependent
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ChainedLoads_Second_Depends_On_First_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string orderId)
    {
        var order = session.Load<Order>(orderId);
        var customer = session.Load<Customer>(order.CustomerId);
    }
}

class Order { public string Id { get; set; } public string CustomerId { get; set; } }
class Customer { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // No diagnostics: only first load is batchable; second depends on it
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task OperationsInsideLambda_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        Action<IDocumentSession> loadUsers = sess =>
        {
            var user1 = sess.Load<User>(""users/1"");
            var user2 = sess.Load<User>(""users/2"");
        };
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // Lambda is a separate code block; no diagnostics here
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task AlreadyLazy_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId)
    {
        var lazyUser = session.Advanced.Lazily.Load<User>(userId);
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // Already using lazy API; no diagnostics
            Assert.Empty(diagnostics);
        }
    }
}
