using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.Subscriptions;
using Xunit;

namespace AnalyzersTests.Subscriptions
{
    public class SubscriptionOpenSessionAnalyzerTests
    {
        private const string CommonUsings = @"
using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
";

        // ── Flag cases ──────────────────────────────────────────────────────────

        [Fact]
        public async Task StoreOpenSession_In_Sync_RunLambda_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            using var session = store.OpenSession();
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
            Assert.Equal(DiagnosticSeverity.Warning, d.Severity);
            Assert.Contains("OpenSession", d.GetMessage());
        }

        [Fact]
        public async Task StoreOpenAsyncSession_In_Async_RunLambda_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker, IDocumentStore store)
    {
        worker.Run(async batch =>
        {
            using var session = store.OpenAsyncSession();
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
            Assert.Equal(DiagnosticSeverity.Warning, d.Severity);
            Assert.Contains("OpenAsyncSession", d.GetMessage());
        }

        [Fact]
        public async Task StoreOpenSessionWithOptions_In_RunLambda_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            using var session = store.OpenSession(new SessionOptions());
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
            Assert.Contains("OpenSession", d.GetMessage());
        }

        [Fact]
        public async Task StoreOpenAsyncSessionWithOptions_In_RunLambda_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker, IDocumentStore store)
    {
        worker.Run(async batch =>
        {
            using var session = store.OpenAsyncSession(new SessionOptions());
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
            Assert.Contains("OpenAsyncSession", d.GetMessage());
        }

        [Fact]
        public async Task FieldStore_OpenSession_In_RunLambda_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyService
{
    private readonly IDocumentStore _store;

    public MyService(IDocumentStore store)
    {
        _store = store;
    }

    public void Subscribe(SubscriptionWorker<Order> worker)
    {
        worker.Run(batch =>
        {
            using var session = _store.OpenSession();
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
            Assert.Contains("OpenSession", d.GetMessage());
        }

        [Fact]
        public async Task Two_OpenSession_Calls_In_RunLambda_Reports_Two_Diagnostics()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            using var session1 = store.OpenSession();
            using var session2 = store.OpenSession();
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Assert.Equal(2, diagnostics.Length);
            Assert.All(diagnostics, d =>
            {
                Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
                Assert.Equal(DiagnosticSeverity.Warning, d.Severity);
            });
        }

        [Fact]
        public async Task OpenSession_In_NestedLambda_Inside_RunLambda_Reports_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Collections.Generic;
using System.Linq;

class Test
{
    void Run(SubscriptionWorker<Order> worker, IDocumentStore store)
    {
        worker.Run(batch =>
        {
            var sessions = new List<int> { 1 }.Select(_ => store.OpenSession());
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
            Assert.Contains("OpenSession", d.GetMessage());
        }

        // ── No-flag cases ────────────────────────────────────────────────────────

        [Fact]
        public async Task BatchOpenSession_Correct_Usage_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker)
    {
        worker.Run(batch =>
        {
            using var session = batch.OpenSession();
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task StoreOpenSession_Outside_RunLambda_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class Test
{
    void DoWork(IDocumentStore store)
    {
        using var session = store.OpenSession();
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task StoreOpenSession_In_Where_Lambda_Not_Run_No_Diagnostic()
        {
            const string source = CommonUsings + @"
using System.Collections.Generic;
using System.Linq;

class Test
{
    void DoWork(IDocumentStore store, IEnumerable<Order> orders)
    {
        var filtered = orders.Where(o =>
        {
            using var session = store.OpenSession();
            return true;
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task BatchTyped_OpenSession_No_False_Positive()
        {
            const string source = CommonUsings + @"
class Test
{
    void Run(SubscriptionWorker<Order> worker)
    {
        worker.Run((SubscriptionBatch<Order> batch) =>
        {
            using var session = batch.OpenSession();
        });
    }
}

class Order { public string Id { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
