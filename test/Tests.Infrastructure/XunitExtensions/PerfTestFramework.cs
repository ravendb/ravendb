using System.Reflection;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerfTestFramework : TestFramework
    {
        public PerfTestFramework(IMessageSink diagnosticMessageSink)
            : base(diagnosticMessageSink)
        {

        }


        protected override ITestFrameworkDiscoverer CreateDiscoverer(IAssemblyInfo assemblyInfo) => 
            new XunitTestFrameworkDiscoverer(assemblyInfo, SourceInformationProvider, DiagnosticMessageSink);

        protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName) => 
            new PerfTestExecutor(assemblyName, SourceInformationProvider, DiagnosticMessageSink);
    }
}
