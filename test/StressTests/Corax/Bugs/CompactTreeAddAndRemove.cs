using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Corax.Bugs;

public class CompactTreeAddAndRemove : NoDisposalNoOutputNeeded
{
    private readonly ITestOutputHelper _testOutputHelper; 
    
    public CompactTreeAddAndRemove(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData("repro-4.log.gz")]
    public void AddAndRemoveValues(string filename)
    {
        using var testClass = new SlowTests.Corax.Bugs.CompactTreeAddAndRemove(Output, _testOutputHelper);
        testClass.AddAndRemoveValues(filename);
    }
}
