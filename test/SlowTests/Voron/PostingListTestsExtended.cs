using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class PostingListTestsExtended : NoDisposalNoOutputNeeded
{
    public PostingListTestsExtended(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(1337, 200000)]
    public void CanDeleteAndInsertInRandomOrder(int seed, int size)
    {
        using var testClass = new FastTests.Voron.Sets.PostingListTests(Output);
        testClass.CanDeleteAndInsertInRandomOrder(seed, size);
    }
}
