using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure.TestMetrics;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestClassRunner : XunitTestClassRunner
    {
        private static readonly TimeSpan TestExecutionSnapshotInterval = TimeSpan.FromMilliseconds(100);
        
        private readonly ITestClass _testClass;
        private readonly TestResourceSnapshotWriter _testResourceSnapshotWriter;
        private readonly bool _resourceSnapshotEnabled;

        public PerformanceTestClassRunner(
            ITestClass testClass, 
            IReflectionTypeInfo @class, 
            IEnumerable<IXunitTestCase> testCases, 
            IMessageSink diagnosticMessageSink, 
            IMessageBus messageBus, 
            ITestCaseOrderer testCaseOrderer, 
            ExceptionAggregator aggregator, 
            CancellationTokenSource cancellationTokenSource, 
            IDictionary<Type, object> collectionFixtureMappings,
            TestResourceSnapshotWriter testResourceSnapshotWriter, 
            in bool resourceSnapshotEnabled) : base(testClass, @class, testCases, diagnosticMessageSink, messageBus, testCaseOrderer, aggregator, cancellationTokenSource, collectionFixtureMappings)
        {
            _testClass = testClass;
            _testResourceSnapshotWriter = testResourceSnapshotWriter;
            _resourceSnapshotEnabled = resourceSnapshotEnabled;
        }

        protected override Task AfterTestClassStartingAsync()
        {
            if (_resourceSnapshotEnabled)
                _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestClassStarted, _testClass);
            
            return base.AfterTestClassStartingAsync();
        }

        protected override Task BeforeTestClassFinishedAsync()
        {
            if (_resourceSnapshotEnabled)
                _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestClassEnded, _testClass);

            return base.BeforeTestClassFinishedAsync();
        }

        protected override Task<RunSummary> RunTestMethodAsync(ITestMethod testMethod, IReflectionMethodInfo method, IEnumerable<IXunitTestCase> testCases, object[] constructorArguments)
        {
            var skipTestResourceSnapshot = _resourceSnapshotEnabled == false || IsTheory(testMethod);
            
            Timer executionSamplingTimer = null;
            var isExecutionSamplingEnabled = IsTestExecutionSamplingEnabled();

            if (skipTestResourceSnapshot == false)
            {
                if (isExecutionSamplingEnabled)
                    executionSamplingTimer = new Timer(WriteTestExecutionSnapshot, testMethod, TestExecutionSnapshotInterval, TestExecutionSnapshotInterval);
                
                _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestStarted, testMethod);
            }

            return base.RunTestMethodAsync(testMethod, method, testCases, constructorArguments)
                .ContinueWith(t =>
                {
                    var runSummary = t.Result;

                    if (skipTestResourceSnapshot)
                        return runSummary;
                    
                    executionSamplingTimer?.Dispose();
                    
                    var testResult = GetTestResult(runSummary);
                    _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestEndedBeforeGc, testMethod, testResult);
                
                    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Forced, true, true);
                    GC.WaitForPendingFinalizers();

                    _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestEndedAfterGc, testMethod, testResult);

                    return runSummary;
                });
        }

        private static readonly Type TheoryType = typeof(TheoryAttribute);

        private static bool IsTheory(ITestMethod testMethod)
        {
            var theoryAttributes = testMethod.Method.GetCustomAttributes(TheoryType); //this will also return all attributes assignable to TheoryAttribute
            return theoryAttributes.Any();
        }

        private static bool IsTestExecutionSamplingEnabled()
            => bool.TryParse(Environment.GetEnvironmentVariable("TEST_RESOURCE_ANALYZER_SAMPLING"), out var value) && value;

        private static TestResult GetTestResult(RunSummary runSummary)
        {
            if (runSummary.Failed > 0)
                return TestResult.Fail;

            return AllTestsWereSkipped(runSummary)
                ? TestResult.Skipped
                : TestResult.Success;
        }
        
        private static bool AllTestsWereSkipped(RunSummary runSummary) => runSummary.Skipped == runSummary.Total;

        private void WriteTestExecutionSnapshot(object timerState)
        {
            var testMethod = timerState as ITestMethod;
            _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestExecution, testMethod);
        }
    }
}
