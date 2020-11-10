using System;
using System.Collections.Generic;
using System.Reflection;
using Tests.Infrastructure.TestMetrics;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestExecutor: XunitTestFrameworkExecutor
    {
        public PerformanceTestExecutor(AssemblyName assemblyName, ISourceInformationProvider sourceInformationProvider, IMessageSink diagnosticMessageSink) : base(assemblyName, sourceInformationProvider, diagnosticMessageSink)
        {
        }

        protected override async void RunTestCases(IEnumerable<IXunitTestCase> testCases, IMessageSink executionMessageSink, ITestFrameworkExecutionOptions executionOptions)
        {
            var resourceSnapshotEnabled = bool.TryParse(Environment.GetEnvironmentVariable("TEST_RESOURCE_ANALYZER_ENABLE"), out var value) && value;
            
            using (var testResourceSnapshotWriter = new TestResourceSnapshotWriter())
            {
                if (resourceSnapshotEnabled)
                    testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestAssemblyStarted, TestAssembly);

                try
                {
                    using var assemblyRunner = new PerformanceTestAssemblyRunner(
                        TestAssembly,
                        testCases,
                        DiagnosticMessageSink,
                        executionMessageSink,
                        executionOptions,
                        testResourceSnapshotWriter,
                        resourceSnapshotEnabled);

                    await assemblyRunner.RunAsync();
                }
                finally
                {
                    if (resourceSnapshotEnabled)
                        testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestAssemblyEnded, TestAssembly);
                }
            }
        }
    }
}
