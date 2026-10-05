using FastTests;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelConnectionHealthTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Reconnecting_clears_the_previous_connection_error()
    {
        var health = new ChannelConnectionHealth();
        health.MarkDisconnected("slack disabled Socket Mode for this app");

        health.MarkConnected();

        Assert.True(health.IsConnected);
        Assert.NotNull(health.LastConnectedAt);
        Assert.Null(health.LastConnectionError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_quiet_disconnect_keeps_the_last_connect_time_and_error()
    {
        var health = new ChannelConnectionHealth();
        health.MarkConnected();
        var connectedAt = health.LastConnectedAt;
        health.MarkDisconnected("discord did not send a hello frame within 00:00:15");

        health.MarkDisconnected(null);

        Assert.False(health.IsConnected);
        Assert.Equal(connectedAt, health.LastConnectedAt);
        Assert.Equal("discord did not send a hello frame within 00:00:15", health.LastConnectionError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_send_failure_records_its_time_with_its_message()
    {
        var health = new ChannelConnectionHealth();

        health.MarkSendFailed("channel_not_found");

        Assert.NotNull(health.LastSendErrorAt);
        Assert.Equal("channel_not_found", health.LastSendError);
        Assert.Null(health.LastInboundAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_successful_send_clears_the_recorded_send_error()
    {
        var health = new ChannelConnectionHealth();
        health.MarkReceived();
        health.MarkSendFailed("503: discord is unavailable");

        health.MarkSendSucceeded();

        Assert.Null(health.LastSendError);
        Assert.Null(health.LastSendErrorAt);
        Assert.NotNull(health.LastInboundAt);
    }
}
