using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class SetTestsExtended : NoDisposalNoOutputNeeded
{
    public SetTestsExtended(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(1337, 200000)]
    public void CanDeleteAndInsertInRandomOrder(int seed, int size)
    {
        using var testClass = new FastTests.Voron.Sets.SetTests(Output);
        testClass.CanDeleteAndInsertInRandomOrder(seed, size);
    }
}
