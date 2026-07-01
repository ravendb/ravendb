using System.Collections.Immutable;
using System.Threading.Tasks;
using AnalyzersTests.Framework;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Subscriptions;
using Xunit;

namespace AnalyzersTests.Subscriptions
{
    // Regression test: RVN011 gates the SubscriptionWorker type match on the Raven.Client namespace
    // (like every other Raven-type check), so a user type merely named SubscriptionWorker does not trip
    // the diagnostic even when a real IDocumentStore session is opened inside its Run lambda.
    public class SubscriptionWorkerTypeGateRegressionTests
    {
        [Fact]
        public async Task User_Type_Named_SubscriptionWorker_Is_Not_Treated_As_Raven_Worker()
        {
            const string source = @"
using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace MyApp
{
    public class SubscriptionWorker<T>
    {
        public void Run(Action<T> action) { }
    }

    class Test
    {
        void Execute(SubscriptionWorker<int> worker, IDocumentStore store)
        {
            worker.Run(batch =>
            {
                var session = store.OpenSession();
            });
        }
    }
}
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<SubscriptionOpenSessionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }
    }
}
