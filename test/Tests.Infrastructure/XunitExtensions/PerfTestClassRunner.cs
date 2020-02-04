using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Xunit.Sdk;
using XunitLogger;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerfTestClassRunner : XunitTestClassRunner
    {
        private readonly ITestClass _testClass;
        private readonly TestResourceSnapshotWriter _testResourceSnapshotWriter;
        private readonly bool _resourceSnapshotEnabled;

        public PerfTestClassRunner(
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
            if(_resourceSnapshotEnabled)
                _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestClassStarted, _testClass.Class.Name);
            return base.AfterTestClassStartingAsync();
        }

        protected override Task BeforeTestClassFinishedAsync()
        {
            if(_resourceSnapshotEnabled)
                _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestClassEnded, _testClass.Class.Name);

            return base.BeforeTestClassFinishedAsync();
        }

        private static string GetDisplayName(ITestMethod testMethod)
            => $"{testMethod.TestClass.Class.Name}::{testMethod.Method.Name}()";

        protected override Task<RunSummary> RunTestMethodAsync(ITestMethod testMethod, IReflectionMethodInfo method, IEnumerable<IXunitTestCase> testCases, object[] constructorArguments)
        {
            if(_resourceSnapshotEnabled)
                _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestStarted, GetDisplayName(testMethod));

            return base.RunTestMethodAsync(testMethod, method, testCases, constructorArguments)
                       .ContinueWith(t =>
                       {
                           if(_resourceSnapshotEnabled)
                               _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestFinishedBeforeGc, GetDisplayName(testMethod));

                           GC.Collect(2, GCCollectionMode.Forced, true, true);

                           if(_resourceSnapshotEnabled)
                               _testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestFinishedAfterGc, GetDisplayName(testMethod));

                           return t.Result;
                       });
        }


    }
}
