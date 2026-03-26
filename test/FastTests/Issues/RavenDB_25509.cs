using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_25509 : NoDisposalNeeded
{
    public RavenDB_25509(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Codebase)]
    public void Assert_Method_Names()
    {
        Assert.Equal("HardResetToPassive", nameof(RachisConsensus.HardResetToPassive));
        Assert.Equal("HardResetToNewCluster", nameof(RachisConsensus.HardResetToNewCluster));
        Assert.Equal("FoundAboutHigherTerm", nameof(RachisConsensus.FoundAboutHigherTerm));
    }
}
