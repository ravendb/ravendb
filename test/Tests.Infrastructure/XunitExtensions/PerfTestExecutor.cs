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
            using var assemblyRunner = new PerfTestAssemblyRunner(
                TestAssembly, 
                testCases, 
                DiagnosticMessageSink, 
                executionMessageSink, 
                executionOptions);
            
            await assemblyRunner.RunAsync();
        }
    }
}
