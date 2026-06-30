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

        [Fact]
        public async Task IncludeStyleLoads_Not_Batchable_No_Diagnostic()
        {
            // Load<T>(id, includes) has no lazy equivalent — Lazily.Load only accepts (id) or
            // (id, Action<T> onEval), never an Action<IIncludeBuilder<T>>. Flagging it would let the
            // code fix copy the include lambda onto Lazily.Load and emit uncompilable code, so the
            // analyzer counts only the single-argument Load(id) form as batchable.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session, string userId, string managerId)
    {
        var user = session.Load<User>(userId, i => i.IncludeDocuments(x => x.ManagerId));
        var manager = session.Load<User>(managerId, i => i.IncludeDocuments(x => x.ManagerId));
    }
}

class User { public string Id { get; set; } public string ManagerId { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task ScalarMaterializers_Not_Batchable_No_Diagnostic()
        {
            // First/Any/Single/Count have no IRavenQueryable.Lazily() equivalent the code fix can
            // produce, so the detection pass excludes them (in lockstep with the fix). Two such calls
            // are therefore not reported, even though both materialize a server round-trip.
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session)
    {
        var first = session.Query<User>().First();
        var any = session.Query<User>().Any();
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task QueryMaterializer_With_Argument_Not_Counted_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    async Task Run(IAsyncDocumentSession session, string managerId, System.Threading.CancellationToken token)
    {
        var employees = await session.Query<Employee>().Where(e => e.Active).ToListAsync(token);
        var manager = await session.LoadAsync<User>(managerId);
    }
}

class Employee { public string Id { get; set; } public bool Active { get; set; } }
class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // ToListAsync(token) carries an argument the lazy rewrite cannot preserve, so it is not
            // counted as batchable (lockstep with the code fix). Only the Load remains → no diagnostic.
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task LoadsOnDifferentSessions_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session1, IDocumentSession session2, string userId, string orderId)
    {
        var user = session1.Load<User>(userId);
        var order = session2.Load<Order>(orderId);
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // Two batchable Loads but on different sessions — cannot share a lazy multi-get.
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task QueriesOnDifferentSessions_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session1, IDocumentSession session2)
    {
        var users = session1.Query<User>().ToList();
        var orders = session2.Query<Order>().ToList();
    }
}

class User { public string Id { get; set; } }
class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // Two materializing queries but on different sessions — cannot share a multi-get.
            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task TwoLoadsOnSession1_PlusOneOnSession2_Reports_Two_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(IDocumentSession session1, IDocumentSession session2, string a, string b, string c)
    {
        var x = session1.Load<User>(a);
        var y = session2.Load<User>(b);
        var z = session1.Load<User>(c);
    }
}

class User { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            // session1 group has 2 loads, session2 has 1 — only the session1 group fires.
            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d => Assert.Equal(DiagnosticIds.SessionLazyBatching, d.Id));
        }

        [Fact]
        public async Task TwoLoads_OnSameNamedNonRavenSession_No_Diagnostic()
        {
            // A user type named IDocumentSession that is NOT in the Raven.Client namespace must not be
            // treated as a RavenDB session, so its repeated Load calls are not flagged for batching.
            const string source = @"
namespace MyApp
{
    public interface IDocumentSession { T Load<T>(string id); }
    public class User { public string Id { get; set; } }

    public class Test
    {
        public void Run(IDocumentSession session, string a, string b)
        {
            var x = session.Load<User>(a);
            var y = session.Load<User>(b);
        }
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SessionLazyBatchingAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
