using System;
using System.Collections.Generic;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure.TestMetrics;
using Xunit;
using Xunit.v3;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestClassRunner : XunitTestClassRunner
    {
        private static readonly TimeSpan TestExecutionSnapshotInterval = TimeSpan.FromMilliseconds(100);

        protected override ValueTask<bool> OnTestClassStarting(XunitTestClassRunnerContext ctxt)
        {
            if (PerformanceTestState.ResourceSnapshotEnabled)
                PerformanceTestState.Writer?.WriteResourceSnapshot(TestStage.TestClassStarted, ctxt.TestClass);

            return base.OnTestClassStarting(ctxt);
        }

        protected override ValueTask<bool> OnTestClassFinished(XunitTestClassRunnerContext ctxt, RunSummary summary)
        {
            if (PerformanceTestState.ResourceSnapshotEnabled)
                PerformanceTestState.Writer?.WriteResourceSnapshot(TestStage.TestClassEnded, ctxt.TestClass);

            return base.OnTestClassFinished(ctxt, summary);
        }

        protected override async ValueTask<RunSummary> RunTestMethod(
            XunitTestClassRunnerContext ctxt,
            IXunitTestMethod testMethod,
            IReadOnlyCollection<IXunitTestCase> testCases,
            object[] constructorArguments)
        {
            var enabled = PerformanceTestState.ResourceSnapshotEnabled;
            var writer = PerformanceTestState.Writer;
            var skipTestResourceSnapshot = !enabled || IsTheory(testMethod);

            Timer executionSamplingTimer = null;
            var isExecutionSamplingEnabled = IsTestExecutionSamplingEnabled();

            if (!skipTestResourceSnapshot && writer != null)
            {
                if (isExecutionSamplingEnabled)
                    executionSamplingTimer = new Timer(
                        state => writer.WriteResourceSnapshot(TestStage.TestExecution, (IXunitTestMethod)state),
                        testMethod,
                        TestExecutionSnapshotInterval,
                        TestExecutionSnapshotInterval);

                writer.WriteResourceSnapshot(TestStage.TestStarted, testMethod);
            }

            var runSummary = await base.RunTestMethod(ctxt, testMethod, testCases, constructorArguments);

            if (!skipTestResourceSnapshot && writer != null)
            {
                executionSamplingTimer?.Dispose();

                var testResult = GetTestResult(runSummary);
                writer.WriteResourceSnapshot(TestStage.TestEndedBeforeGc, testMethod, testResult);

                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect(2, GCCollectionMode.Forced, true, true);
                GC.WaitForPendingFinalizers();

                writer.WriteResourceSnapshot(TestStage.TestEndedAfterGc, testMethod, testResult);
            }

            return runSummary;
        }

        private static readonly Type TheoryType = typeof(TheoryAttribute);

        private static bool IsTheory(IXunitTestMethod testMethod)
        {
            return testMethod.Method.GetCustomAttributes(TheoryType, true).Length > 0;
        }

        private static bool IsTestExecutionSamplingEnabled()
            => bool.TryParse(Environment.GetEnvironmentVariable("TEST_RESOURCE_ANALYZER_SAMPLING"), out var value) && value;

        private static TestMetrics.TestResult GetTestResult(RunSummary runSummary)
        {
            if (runSummary.Failed > 0)
                return TestMetrics.TestResult.Fail;

            return AllTestsWereSkipped(runSummary)
                ? TestMetrics.TestResult.Skipped
                : TestMetrics.TestResult.Success;
        }

        private static bool AllTestsWereSkipped(RunSummary runSummary) => runSummary.Skipped == runSummary.Total;
    }
}
