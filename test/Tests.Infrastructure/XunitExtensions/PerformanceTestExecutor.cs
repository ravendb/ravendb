using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure.TestMetrics;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestExecutor : XunitTestFrameworkExecutor
    {
        public PerformanceTestExecutor(IXunitTestAssembly testAssembly) : base(testAssembly)
        {
        }

        public override async ValueTask RunTestCases(
            IReadOnlyCollection<IXunitTestCase> testCases,
            IMessageSink executionMessageSink,
            ITestFrameworkExecutionOptions executionOptions,
            CancellationToken cancellationToken)
        {
            var resourceSnapshotEnabled = RavenTestHelper.EnvironmentVariables.TestResourceAnalyzerEnable;

            using var testResourceSnapshotWriter = new TestResourceSnapshotWriter();
            PerformanceTestState.Writer = testResourceSnapshotWriter;
            PerformanceTestState.ResourceSnapshotEnabled = resourceSnapshotEnabled;

            if (resourceSnapshotEnabled)
                testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestAssemblyStarted, TestAssembly);

            try
            {
                var runner = new PerformanceTestAssemblyRunner();
                await runner.Run(TestAssembly, testCases, executionMessageSink, executionOptions, cancellationToken);
            }
            finally
            {
                if (resourceSnapshotEnabled)
                    testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestAssemblyEnded, TestAssembly);

                PerformanceTestState.Writer = null;
            }
        }
    }

    internal static class PerformanceTestState
    {
        internal static TestResourceSnapshotWriter Writer;
        internal static bool ResourceSnapshotEnabled;
    }
}
