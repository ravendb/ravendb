using System.Collections.Concurrent;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Channels;
using Raven.Quill.Logging;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelChatsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Messages_posted_during_a_turn_are_drained_together_into_the_next_turn()
    {
        await using var chats = new TestChats();

        chats.Send("a", "m0");
        await chats.WaitEnteredAsync();

        chats.Send("a", "m1");
        chats.Send("a", "m2");
        chats.Send("a", "m3");
        chats.Proceed.Release();

        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        Assert.Equal(new[] { new[] { "m0" }, new[] { "m1", "m2", "m3" } }, chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Different_senders_run_in_parallel()
    {
        await using var chats = new TestChats();

        chats.Send("a", "a1");
        chats.Send("b", "b1");

        await chats.WaitEnteredAsync();
        await chats.WaitEnteredAsync();
        chats.Proceed.Release(2);

        Assert.Equal(new[] { "a", "b" }, chats.Turns.Select(t => t.SenderId).Order());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Messages_from_different_channels_with_the_same_sender_get_separate_chats()
    {
        await using var chats = new TestChats();

        chats.Send("a", "x", channelId: "channels/c1");
        chats.Send("a", "y", channelId: "channels/c2");

        await chats.WaitEnteredAsync();
        await chats.WaitEnteredAsync();
        Assert.Equal(2, chats.ActiveChatCount);
        chats.Proceed.Release(2);

        Assert.Equal(new[] { "x", "y" }, chats.Turns.SelectMany(t => t.Texts).Order());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Capacity_counts_only_queued_messages()
    {
        await using var chats = new TestChats(capacity: 2);

        chats.Send("a", "hold");
        await chats.WaitEnteredAsync();

        chats.Send("a", "q1");
        chats.Send("a", "q2");
        Assert.Empty(chats.Notices);

        chats.Send("a", "q3");
        Assert.Equal(new[] { ("a", "q3") }, chats.Notices);

        chats.Proceed.Release();
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        Assert.Equal(new[] { new[] { "hold" }, new[] { "q1", "q2" } }, chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_first_drop_notifies_and_the_next_notice_waits_for_a_turn_to_finish()
    {
        await using var chats = new TestChats(capacity: 1);

        chats.Send("a", "hold");
        await chats.WaitEnteredAsync();

        chats.Send("a", "q1");
        chats.Send("a", "d1");
        chats.Send("a", "d2");
        Assert.Equal(new[] { "d1" }, chats.Notices.Select(n => n.Text));

        chats.Proceed.Release();
        await chats.WaitEnteredAsync();

        chats.Send("a", "q2");
        chats.Send("a", "d3");
        Assert.Equal(new[] { "d1", "d3" }, chats.Notices.Select(n => n.Text));

        chats.Proceed.Release();
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_run_alone_message_flushes_the_merge_and_gets_its_own_turn()
    {
        await using var chats = new TestChats();

        chats.Send("a", "hold");
        await chats.WaitEnteredAsync();

        chats.Send("a", "t1");
        chats.Send("a", "t2");
        chats.Send("a", "/cmd", runsAlone: true);
        chats.Send("a", "t3");
        chats.Proceed.Release();

        for (var turn = 0; turn < 3; turn++)
        {
            await chats.WaitEnteredAsync();
            chats.Proceed.Release();
        }

        Assert.Equal(
            new[] { new[] { "hold" }, new[] { "t1", "t2" }, new[] { "/cmd" }, new[] { "t3" } },
            chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Blank_texts_are_skipped()
    {
        await using var chats = new TestChats();

        chats.Send("a", "hold");
        await chats.WaitEnteredAsync();

        chats.Send("a", " ");
        chats.Send("a", "a1");
        chats.Send("a", "");
        chats.Proceed.Release();

        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        Assert.Equal(new[] { new[] { "hold" }, new[] { "a1" } }, chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_throwing_turn_does_not_stop_the_chat()
    {
        await using var chats = new TestChats(throwOn: "boom");

        chats.Send("a", "boom");
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        chats.Send("a", "ok");
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        Assert.Equal(new[] { new[] { "boom" }, new[] { "ok" } }, chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failing_turn_does_not_skip_the_rest_of_the_batch()
    {
        await using var chats = new TestChats(throwOn: "boom");

        chats.Send("a", "hold");
        await chats.WaitEnteredAsync();

        chats.Send("a", "t1");
        chats.Send("a", "boom", runsAlone: true);
        chats.Send("a", "t2");
        chats.Proceed.Release();

        for (var turn = 0; turn < 3; turn++)
        {
            await chats.WaitEnteredAsync();
            chats.Proceed.Release();
        }

        Assert.Equal(
            new[] { new[] { "hold" }, new[] { "t1" }, new[] { "boom" }, new[] { "t2" } },
            chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_idle_chat_retires_and_the_next_message_starts_a_new_one()
    {
        await using var chats = new TestChats(idleTimeout: TimeSpan.FromMilliseconds(200));

        chats.Send("a", "m1");
        Assert.Equal(1, chats.ActiveChatCount);
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        await MockApiWait.UntilAsync(nameof(ChannelChatsTests), () => chats.ActiveChatCount == 0, "the idle chat to retire", WaitTimeout);

        chats.Send("a", "m2");
        Assert.Equal(1, chats.ActiveChatCount);
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();

        Assert.Equal(new[] { new[] { "m1" }, new[] { "m2" } }, chats.Turns.Select(t => t.Texts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_waits_for_the_running_turn()
    {
        await using var chats = new TestChats();

        chats.Send("a", "m1");
        await chats.WaitEnteredAsync();

        var stop = chats.StopAsync();
        Assert.False(stop.IsCompleted);

        chats.Proceed.Release();
        await stop.WaitAsync(WaitTimeout);
    }

    private sealed record TestMessage(string SenderId, string? Text, bool RunsAlone, Channel Channel)
        : IChannelMessage
    {
        public string Database => "db";

        public bool IsUnsupported => false;
    }

    private sealed class TestChats : IChatTurns<TestMessage>, IAsyncDisposable
    {
        private readonly SemaphoreSlim _entered = new(0);
        private readonly ChannelChats<TestMessage> _chats;
        private readonly string? _throwOn;

        public TestChats(int capacity = 10, TimeSpan? idleTimeout = null, string? throwOn = null)
        {
            _throwOn = throwOn;
            _chats = new ChannelChats<TestMessage>(
                this, new QuillLogger<ChannelChatsTests>().RavenLogger, capacity,
                idleTimeout ?? TimeSpan.FromMinutes(1));
        }

        public ValueTask DisposeAsync() => new(StopAsync());

        public ConcurrentQueue<(string SenderId, string[] Texts)> Turns { get; } = new();

        public ConcurrentQueue<(string SenderId, string Text)> Notices { get; } = new();

        public SemaphoreSlim Proceed { get; } = new(0);

        public int ActiveChatCount => _chats.ActiveChatCount;

        public void Send(string senderId, string text, bool runsAlone = false, string channelId = "channels/c1") =>
            _chats.Enqueue(new TestMessage(senderId, text, runsAlone, new Channel { Id = channelId }));

        public Task StopAsync() => _chats.StopAsync(CancellationToken.None);

        public async Task WaitEnteredAsync() =>
            Assert.True(await _entered.WaitAsync(WaitTimeout), "no turn started");

        public async Task RunTurnAsync(IReadOnlyList<TestMessage> messages, CancellationToken ct)
        {
            Turns.Enqueue((messages[0].SenderId, messages.Select(m => m.Text!).ToArray()));
            _entered.Release();
            await Proceed.WaitAsync();

            if (_throwOn is not null && messages.Any(m => m.Text == _throwOn))
                throw new InvalidOperationException("turn failed");
        }

        public Task NotifyBufferFullAsync(TestMessage message, CancellationToken ct)
        {
            Notices.Enqueue((message.SenderId, message.Text!));
            return Task.CompletedTask;
        }
    }
}
