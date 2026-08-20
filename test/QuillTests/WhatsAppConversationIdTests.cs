using FastTests;
using Raven.Quill.Agents;
using Raven.Quill.WhatsApp;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WhatsAppConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string ChannelId = "0f8fad5b0d1d4d0dbc9df1b0a3e0c9a1";
    private const string SenderJid = "48123456789@s.whatsapp.net";

    [RavenFact(RavenTestCategory.Quill)]
    public void Derives_chats_prefixed_id_with_channel_sender_and_utc_date()
    {
        var id = WhatsAppConversationId.For(ChannelId, SenderJid, new DateTime(2026, 8, 5, 13, 30, 0, DateTimeKind.Utc));

        Assert.Equal($"chats/wa/{ChannelId}/48123456789/2026-08-05", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Device_suffix_does_not_change_the_id()
    {
        var utcNow = new DateTime(2026, 8, 5, 13, 30, 0, DateTimeKind.Utc);

        Assert.Equal(
            WhatsAppConversationId.For(ChannelId, SenderJid, utcNow),
            WhatsAppConversationId.For(ChannelId, "48123456789:5@s.whatsapp.net", utcNow));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Same_sender_and_day_derives_the_same_id()
    {
        var morning = WhatsAppConversationId.For(ChannelId, SenderJid, new DateTime(2026, 8, 5, 0, 0, 1, DateTimeKind.Utc));
        var evening = WhatsAppConversationId.For(ChannelId, SenderJid, new DateTime(2026, 8, 5, 23, 59, 59, DateTimeKind.Utc));

        Assert.Equal(morning, evening);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Day_rollover_derives_a_new_id()
    {
        var beforeMidnight = WhatsAppConversationId.For(ChannelId, SenderJid, new DateTime(2026, 8, 5, 23, 59, 59, DateTimeKind.Utc));
        var afterMidnight = WhatsAppConversationId.For(ChannelId, SenderJid, new DateTime(2026, 8, 6, 0, 0, 0, DateTimeKind.Utc));

        Assert.NotEqual(beforeMidnight, afterMidnight);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Different_senders_derive_different_ids()
    {
        var utcNow = new DateTime(2026, 8, 5, 12, 0, 0, DateTimeKind.Utc);

        Assert.NotEqual(
            WhatsAppConversationId.For(ChannelId, SenderJid, utcNow),
            WhatsAppConversationId.For(ChannelId, "48987654321@s.whatsapp.net", utcNow));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Derived_id_passes_the_router_normalizer_and_never_ends_with_a_separator()
    {
        var id = WhatsAppConversationId.For(ChannelId, SenderJid, DateTime.UtcNow);

        Assert.True(AgentRouter.TryNormalizeConversationId(id, out var normalized, out var error));
        Assert.Null(error);
        Assert.Equal(id, normalized);
        Assert.False(id.EndsWith('/'));
        Assert.False(id.EndsWith('|'));
    }
}
