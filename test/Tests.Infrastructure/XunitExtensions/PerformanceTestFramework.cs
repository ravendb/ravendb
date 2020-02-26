using System.Reflection;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestFramework : TestFramework
    {
        public PerformanceTestFramework(IMessageSink diagnosticMessageSink)
            : base(diagnosticMessageSink)
        {

        }


        protected override ITestFrameworkDiscoverer CreateDiscoverer(IAssemblyInfo assemblyInfo) => 
            new XunitTestFrameworkDiscoverer(assemblyInfo, SourceInformationProvider, DiagnosticMessageSink);

        protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName) => 
            new PerformanceTestExecutor(assemblyName, SourceInformationProvider, DiagnosticMessageSink);
    }
}
