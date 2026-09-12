using FastTests;
using Raven.Quill.Discord;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordHealthRegistryTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string Database = "demo";
    private const string ChannelId = "dsc0001";

    [RavenFact(RavenTestCategory.Quill)]
    public void A_terminal_disconnect_does_not_resurrect_an_evicted_entry()
    {
        var health = new DiscordHealthRegistry();
        health.RecordGatewayConnected(Database, ChannelId);
        health.RecordSendError(Database, ChannelId, "503: discord is unavailable");

        health.Remove(Database, ChannelId);
        health.RecordGatewayDisconnected(Database, ChannelId, null);
        health.RecordSendError(Database, ChannelId, "a late in-flight reply");

        var snapshot = health.SnapshotFor(Database, ChannelId);
        Assert.Null(snapshot.LastSendError);
        Assert.Null(snapshot.LastConnectedAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_failed_first_attempt_records_its_error_without_a_prior_entry()
    {
        var health = new DiscordHealthRegistry();

        health.RecordGatewayDisconnected(Database, ChannelId, "discord did not send a hello frame within 00:00:15");

        Assert.Equal(
            "discord did not send a hello frame within 00:00:15",
            health.SnapshotFor(Database, ChannelId).LastGatewayError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_successful_send_clears_the_recorded_send_error()
    {
        var health = new DiscordHealthRegistry();
        health.RecordInbound(Database, ChannelId);
        health.RecordSendError(Database, ChannelId, "503: discord is unavailable");

        health.RecordSendSuccess(Database, ChannelId);

        var snapshot = health.SnapshotFor(Database, ChannelId);
        Assert.Null(snapshot.LastSendError);
        Assert.Null(snapshot.LastSendErrorAt);
    }
}
