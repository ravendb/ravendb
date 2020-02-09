using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerfTestExecutor: XunitTestFrameworkExecutor
    {
        public PerfTestExecutor(AssemblyName assemblyName, ISourceInformationProvider sourceInformationProvider, IMessageSink diagnosticMessageSink) : base(assemblyName, sourceInformationProvider, diagnosticMessageSink)
        {
        }

        protected override async void RunTestCases(IEnumerable<IXunitTestCase> testCases, IMessageSink executionMessageSink, ITestFrameworkExecutionOptions executionOptions)
        {
            var resourceSnapshotEnabled = true; // bool.TryParse(Environment.GetEnvironmentVariable("TEST_RESOURCE_ANALYZER_ENABLE"), out var value) && value;
            using var testResourceSnapshotWriter = new TestResourceSnapshotWriter();

            if(resourceSnapshotEnabled)
                testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestAssemblyStarted, TestAssembly.Assembly.Name);
            try
            {
                using var assemblyRunner = new PerfTestAssemblyRunner(
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
                    testResourceSnapshotWriter.WriteResourceSnapshot(TestStage.TestAssemblyEnded, TestAssembly.Assembly.Name);
            }
        }
    }
}
