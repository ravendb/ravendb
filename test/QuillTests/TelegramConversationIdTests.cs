using FastTests;
using Raven.Quill.Agents;
using Raven.Quill.Telegram;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class TelegramConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string ChannelId = "0f8fad5b0d1d4d0dbc9df1b0a3e0c9a1";

    [RavenFact(RavenTestCategory.Quill)]
    public void Derives_chats_prefixed_id_with_channel_chat_and_utc_date()
    {
        var id = TelegramConversationId.For(ChannelId, 42, new DateTime(2026, 8, 4, 13, 30, 0, DateTimeKind.Utc));

        Assert.Equal($"chats/tg/{ChannelId}/42/2026-08-04", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Same_chat_and_day_derives_the_same_id()
    {
        var morning = TelegramConversationId.For(ChannelId, 42, new DateTime(2026, 8, 4, 0, 0, 1, DateTimeKind.Utc));
        var evening = TelegramConversationId.For(ChannelId, 42, new DateTime(2026, 8, 4, 23, 59, 59, DateTimeKind.Utc));

        Assert.Equal(morning, evening);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Day_rollover_derives_a_new_id()
    {
        var beforeMidnight = TelegramConversationId.For(ChannelId, 42, new DateTime(2026, 8, 4, 23, 59, 59, DateTimeKind.Utc));
        var afterMidnight = TelegramConversationId.For(ChannelId, 42, new DateTime(2026, 8, 5, 0, 0, 0, DateTimeKind.Utc));

        Assert.NotEqual(beforeMidnight, afterMidnight);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Negative_group_chat_ids_are_preserved()
    {
        var id = TelegramConversationId.For(ChannelId, -1001234567890, new DateTime(2026, 8, 4, 12, 0, 0, DateTimeKind.Utc));

        Assert.Equal($"chats/tg/{ChannelId}/-1001234567890/2026-08-04", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Derived_id_passes_the_router_normalizer_and_never_ends_with_a_separator()
    {
        var id = TelegramConversationId.For(ChannelId, 42, DateTime.UtcNow);

        Assert.True(AgentRouter.TryNormalizeConversationId(id, out var normalized, out var error));
        Assert.Null(error);
        Assert.Equal(id, normalized);
        // AiConversation.Id throws on ids ending with '/' or '|' (server-side allocation markers)
        Assert.False(id.EndsWith('/'));
        Assert.False(id.EndsWith('|'));
    }
}

public class TelegramMessageSplitterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Short_text_is_returned_unchanged()
    {
        var parts = TelegramMessageSplitter.Split("hello world");

        Assert.Equal(["hello world"], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Text_at_exactly_the_limit_is_not_split()
    {
        var text = new string('a', 4096);

        Assert.Equal([text], TelegramMessageSplitter.Split(text));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Long_text_splits_at_the_last_sentence_boundary()
    {
        var first = new string('a', 20) + ".";
        var second = new string('b', 20);
        var parts = TelegramMessageSplitter.Split(first + " " + second, limit: 30);

        Assert.Equal([first, second], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Newlines_count_as_sentence_boundaries()
    {
        var first = new string('a', 20);
        var second = new string('b', 20);
        var parts = TelegramMessageSplitter.Split(first + "\n" + second, limit: 30);

        Assert.Equal([first, second], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Text_without_boundaries_hard_splits_at_the_limit()
    {
        var text = new string('a', 70);
        var parts = TelegramMessageSplitter.Split(text, limit: 30);

        Assert.Equal([new string('a', 30), new string('a', 30), new string('a', 10)], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Every_part_stays_within_the_limit()
    {
        var text = string.Join(" ", Enumerable.Range(0, 300).Select(i => $"Sentence number {i} ends here."));
        var parts = TelegramMessageSplitter.Split(text);

        Assert.True(parts.Count > 1);
        Assert.All(parts, part => Assert.True(part.Length <= 4096));
        Assert.All(parts, part => Assert.False(string.IsNullOrWhiteSpace(part)));
    }
}
