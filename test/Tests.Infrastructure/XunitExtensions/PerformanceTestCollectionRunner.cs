using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit.v3;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestCollectionRunner : XunitTestCollectionRunner
    {
        protected override ValueTask<RunSummary> RunTestClass(
            XunitTestCollectionRunnerContext ctxt,
            IXunitTestClass testClass,
            IReadOnlyCollection<IXunitTestCase> testCases)
        {
            var runner = new PerformanceTestClassRunner();
            return runner.Run(
                testClass,
                testCases,
                ctxt.ExplicitOption,
                ctxt.MessageBus,
                ctxt.TestCaseOrderer,
                ctxt.Aggregator,
                ctxt.CancellationTokenSource,
                ctxt.CollectionFixtureMappings);
        }
    }
}
