using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers;
using Raven.Analyzers.CodeFixes.Subscriptions;
using Raven.Analyzers.Subscriptions;
using Xunit;

namespace AnalyzersTests.Subscriptions
{
    // Regression test: the code fix must validate (via the semantic model) that the enclosing Run
    // lambda belongs to a subscription worker before rewriting the receiver, so it never substitutes
    // the parameter of an unrelated method that merely happens to be named Run.
    public class SubscriptionOpenSessionRegressionTests
    {
        private const string CommonUsings = @"
using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
";

        private const string NestedUnrelatedRun = CommonUsings + @"
class Helper { public void Run(Action<int> work) { } }

class Test
{
    void Run(SubscriptionWorker<Doc> worker, IDocumentStore store, Helper helper)
    {
        worker.Run(batch =>
        {
            helper.Run(item =>
            {
                var session = store.OpenSession();
            });
        });
    }
}

class Doc { public string Id { get; set; } }
";

        [Fact]
        public async Task Diagnostic_Still_Reported_When_OpenSession_Is_Nested_In_Unrelated_Run()
        {
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(NestedUnrelatedRun);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);
        }

        [Fact]
        public async Task No_Fix_Rewrites_To_An_Unrelated_Run_Lambda_Parameter()
        {
            // The inner lambda belongs to Helper.Run (not a subscription worker), so the fix must not
            // rewrite store.OpenSession() to item.OpenSession(); it bails and offers no fix.
            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(NestedUnrelatedRun));

            Assert.Contains("No code fixes", ex.Message);
        }

        [Fact]
        public async Task OpenSession_In_Select_Projection_Inside_RunLambda_Flags_But_Fix_Bails()
        {
            // Pins the intended asymmetry: store.OpenSession() inside a nested .Select(...) projection
            // lambda that is itself inside worker.Run(batch => ...) still runs during batch processing,
            // so the diagnostic fires. But the batch parameter is not the directly-enclosing lambda's
            // parameter, and a mechanical receiver swap into the projection could bind wrong or defer
            // execution past the batch, so the fix declines. A diagnostic without an auto-fix is the
            // correct outcome — the developer must restructure by hand.
            const string source = CommonUsings + @"
using System.Collections.Generic;
using System.Linq;

class Test
{
    void Run(SubscriptionWorker<Doc> worker, IDocumentStore store, List<int> docs)
    {
        worker.Run(batch =>
        {
            var ids = docs.Select(d => store.OpenSession()).ToList();
        });
    }
}

class Doc { public string Id { get; set; } }
";

            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.SubscriptionStoreOpenSession, d.Id);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => RavenCodeFixTest.ApplyFixAsync<SubscriptionOpenSessionAnalyzer, SubscriptionOpenSessionCodeFixProvider>(source));

            Assert.Contains("No code fixes", ex.Message);
        }
    }
}
