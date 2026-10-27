using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_22561_LLT : NoDisposalNeeded
{
    public RavenDB_22561_LLT(ITestOutputHelper output) : base(output)
    {
    }

    // the smaller cases run in FastTests.Corax.RavenDB_22561_LLT
    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed(10_000)]
    [InlineData(10000, 52744038)]
    public void CanHeapSortDataString(int size, int seed) => FastTests.Corax.RavenDB_22561_LLT.AssertHeapSortDataString(size, seed);
}
