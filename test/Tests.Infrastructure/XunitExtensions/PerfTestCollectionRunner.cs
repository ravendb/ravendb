using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerfTestCollectionRunner : XunitTestCollectionRunner 
    {
        private readonly TestResourceSnapshotWriter _testResourceSnapshotWriter;
        private readonly bool _resourceSnapshotEnabled;

        public PerfTestCollectionRunner(ITestCollection testCollection,
            IEnumerable<IXunitTestCase> testCases,
            IMessageSink diagnosticMessageSink,
            IMessageBus messageBus,
            ITestCaseOrderer testCaseOrderer,
            ExceptionAggregator aggregator,
            CancellationTokenSource cancellationTokenSource, 
            TestResourceSnapshotWriter testResourceSnapshotWriter, in bool resourceSnapshotEnabled) : 
            base(testCollection,
                 testCases,
                 diagnosticMessageSink,
                 messageBus,
                 testCaseOrderer,
                 aggregator,
                 cancellationTokenSource)
        {
            _testResourceSnapshotWriter = testResourceSnapshotWriter;
            _resourceSnapshotEnabled = resourceSnapshotEnabled;
        }

        protected override Task<RunSummary> RunTestClassAsync(ITestClass testClass, IReflectionTypeInfo @class, IEnumerable<IXunitTestCase> testCases)
            => new PerfTestClassRunner(testClass, @class, testCases, DiagnosticMessageSink, MessageBus, TestCaseOrderer, new ExceptionAggregator(Aggregator), CancellationTokenSource, CollectionFixtureMappings, _testResourceSnapshotWriter, _resourceSnapshotEnabled).RunAsync();
    }
}
