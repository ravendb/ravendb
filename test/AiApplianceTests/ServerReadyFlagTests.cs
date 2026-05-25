using FastTests;
using Raven.AiAppliance.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class ServerReadyFlagTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Starts_not_ready()
    {
        IServerReady flag = new ServerReadyFlag();
        Assert.False(flag.IsReady);
        Assert.Null(flag.LastError);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void MarkReady_flips_state_and_clears_error()
    {
        IServerReady flag = new ServerReadyFlag();
        flag.MarkFailed("boom");
        flag.MarkReady();
        Assert.True(flag.IsReady);
        Assert.Null(flag.LastError);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void MarkFailed_records_error_and_resets_ready()
    {
        IServerReady flag = new ServerReadyFlag();
        flag.MarkReady();
        flag.MarkFailed("connection refused");
        Assert.False(flag.IsReady);
        Assert.Equal("connection refused", flag.LastError);
    }
}
