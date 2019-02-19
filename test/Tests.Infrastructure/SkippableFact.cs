using Xunit;
using Xunit.Sdk;

namespace FastTests
{
    [XunitTestCaseDiscoverer("FastTests.SkippableFactDiscoverer", "FastTests")]
    public class SkippableFactAttribute : FactAttribute { }
}
