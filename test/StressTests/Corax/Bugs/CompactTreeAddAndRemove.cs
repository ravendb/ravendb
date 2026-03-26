using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Corax.Bugs;

public class CompactTreeAddAndRemove : NoDisposalNoOutputNeeded
{
    public CompactTreeAddAndRemove(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData("repro-4.log.gz")]
    public async Task AddAndRemoveValues(string filename)
    {
        await using var testClass = new SlowTests.Corax.Bugs.CompactTreeAddAndRemove(Output);
        testClass.AddAndRemoveValues(filename);
    }
}
