using FastTests;
using Sparrow.Binary;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Utils;

public class NextAllocationSizeTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Memory)]
    public void EnsureBitsNextAllocationSizeIsGreaterOrEqualRequestedSize()
    {
        var previousAllocationSize = -1L;
        for (int i = 1; i < int.MaxValue; i++)
        {
            var nextAllocationSize = Bits.NextAllocationSize(i);
            Assert.True(0 < nextAllocationSize);
            Assert.True(i <= nextAllocationSize);
            Assert.True(previousAllocationSize <= nextAllocationSize);
            previousAllocationSize = nextAllocationSize;
        }
    }
}
