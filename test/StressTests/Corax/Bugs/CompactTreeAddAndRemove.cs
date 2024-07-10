using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Corax.Bugs;

public class CompactTreeAddAndRemove : NoDisposalNoOutputNeeded
{
    public CompactTreeAddAndRemove(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData("repro-4.log.gz")]
    public void AddAndRemoveValues(string filename)
    {
        using var testClass = new SlowTests.Corax.Bugs.CompactTreeAddAndRemove(Output);
        testClass.AddAndRemoveValues(filename);
    }
}
