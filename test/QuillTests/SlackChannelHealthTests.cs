using FastTests;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackChannelHealthTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Reconnecting_clears_the_previous_socket_error()
    {
        var health = new SlackChannelHealth();
        health.Exited("slack disabled Socket Mode for this app");

        health.Connected();

        Assert.True(health.SocketConnected);
        Assert.NotNull(health.LastConnectedAt);
        Assert.Null(health.LastSocketError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_quiet_exit_keeps_the_last_connect_time()
    {
        var health = new SlackChannelHealth();
        health.Connected();
        var connectedAt = health.LastConnectedAt;

        health.Exited(null);

        Assert.False(health.SocketConnected);
        Assert.Null(health.LastSocketError);
        Assert.Equal(connectedAt, health.LastConnectedAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_send_failure_records_its_time_with_its_message()
    {
        var health = new SlackChannelHealth();

        health.SendFailed("channel_not_found");

        Assert.NotNull(health.LastSendErrorAt);
        Assert.Equal("channel_not_found", health.LastSendError);
        Assert.Null(health.LastInboundAt);
    }
}
