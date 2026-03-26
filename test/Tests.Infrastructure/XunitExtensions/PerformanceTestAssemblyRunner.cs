using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit.v3;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestAssemblyRunner : XunitTestAssemblyRunner
    {
        protected override ValueTask<RunSummary> RunTestCollection(
            XunitTestAssemblyRunnerContext ctxt,
            IXunitTestCollection testCollection,
            IReadOnlyCollection<IXunitTestCase> testCases)
        {
            var runner = new PerformanceTestCollectionRunner();
            return runner.Run(
                testCollection,
                testCases,
                ctxt.ExplicitOption,
                ctxt.MessageBus,
                ctxt.AssemblyTestCaseOrderer,
                ctxt.Aggregator,
                ctxt.CancellationTokenSource,
                ctxt.AssemblyFixtureMappings);
        }
    }
}
