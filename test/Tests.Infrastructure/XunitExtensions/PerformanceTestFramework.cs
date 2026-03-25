using System.Reflection;
using Xunit.v3;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerformanceTestFramework : XunitTestFramework
    {
        protected override ITestFrameworkExecutor CreateExecutor(Assembly assembly)
            => new PerformanceTestExecutor(new XunitTestAssembly(assembly));
    }
}
