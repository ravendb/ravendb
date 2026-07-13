using FastTests;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ServerReadyFlagTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Starts_not_ready()
    {
        IServerReady flag = new ServerReadyFlag();
        Assert.False(flag.IsReady);
        Assert.Null(flag.LastError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void MarkReady_flips_state_and_clears_error()
    {
        IServerReady flag = new ServerReadyFlag();
        flag.MarkFailed("boom");
        flag.MarkReady();
        Assert.True(flag.IsReady);
        Assert.Null(flag.LastError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void MarkFailed_records_error_and_resets_ready()
    {
        IServerReady flag = new ServerReadyFlag();
        flag.MarkReady();
        flag.MarkFailed("connection refused");
        Assert.False(flag.IsReady);
        Assert.Equal("connection refused", flag.LastError);
    }
}
