using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure.TestMetrics;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestAssemblyRunner : XunitTestAssemblyRunner
    {
        private readonly TestResourceSnapshotWriter _testResourceSnapshotWriter;
        private readonly bool _resourceSnapshotEnabled;

        public PerformanceTestAssemblyRunner(ITestAssembly testAssembly,
            IEnumerable<IXunitTestCase> testCases,
            IMessageSink diagnosticMessageSink,
            IMessageSink executionMessageSink,
            ITestFrameworkExecutionOptions executionOptions,
            TestResourceSnapshotWriter testResourceSnapshotWriter, 
            in bool resourceSnapshotEnabled) : base(testAssembly,
            testCases,
            diagnosticMessageSink,
            executionMessageSink,
            executionOptions)
        {
            _testResourceSnapshotWriter = testResourceSnapshotWriter;
            _resourceSnapshotEnabled = resourceSnapshotEnabled;
        }

        protected override Task<RunSummary> RunTestCollectionAsync(IMessageBus messageBus, ITestCollection testCollection, IEnumerable<IXunitTestCase> testCases, CancellationTokenSource cancellationTokenSource)
            => new PerformanceTestCollectionRunner(testCollection, testCases, DiagnosticMessageSink, messageBus, TestCaseOrderer, new ExceptionAggregator(Aggregator), cancellationTokenSource, _testResourceSnapshotWriter, _resourceSnapshotEnabled).RunAsync();
    }
}
