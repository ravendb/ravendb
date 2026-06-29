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
    }
}
