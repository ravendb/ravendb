using Xunit;

namespace FastTests
{
    // In xUnit v3, dynamic test skipping is built-in via Assert.Skip().
    // No custom test case discoverer is needed.
    public class SkippableFactAttribute : FactAttribute { }
}
