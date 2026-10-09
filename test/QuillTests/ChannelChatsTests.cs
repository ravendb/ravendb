using System.Collections.Concurrent;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
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
    public async Task Stop_cancels_the_running_turn_and_drops_the_waiting_messages()
    {
        await using var chats = new TestChats();

        chats.Send("a", "m1");
        await chats.WaitEnteredAsync();
        chats.Send("a", "m2");

        await chats.StopAsync().WaitAsync(WaitTimeout);

        Assert.Equal(new[] { new[] { "m1" } }, chats.Turns.Select(t => t.Texts));
        Assert.Equal(0, chats.ActiveChatCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_retires_every_chat_and_later_messages_are_dropped()
    {
        await using var chats = new TestChats();

        chats.Send("a", "m1");
        await chats.WaitEnteredAsync();
        chats.Proceed.Release();
        await chats.StopAsync().WaitAsync(WaitTimeout);

        Assert.Equal(0, chats.ActiveChatCount);

        chats.Send("a", "late");
        Assert.Equal(0, chats.ActiveChatCount);
        Assert.Single(chats.Turns);
    }

    private sealed record TestMessage(string SenderId, string? Text, Channel Channel) : IChannelMessage
    {
        public string Database => "db";

        public bool RunsAlone => false;

        public bool IsUnsupported => false;
    }

    private sealed class TestChats : IChatTurns<TestMessage>, IAsyncDisposable
    {
        private readonly SemaphoreSlim _entered = new(0);
        private readonly ChannelChats<TestMessage> _chats;

        public TestChats(int capacity = 10, TimeSpan? idleTimeout = null)
        {
            _chats = new ChannelChats<TestMessage>(
                this,
                new ApplianceOptions
                {
                    ChannelSenderQueueCapacity = capacity,
                    ChannelSenderIdleTimeout = idleTimeout ?? TimeSpan.FromMinutes(1),
                },
                new QuillLogger<ChannelChatsTests>().RavenLogger);
        }

        public ValueTask DisposeAsync() => new(StopAsync());

        public ConcurrentQueue<(string SenderId, string[] Texts)> Turns { get; } = new();

        public ConcurrentQueue<(string SenderId, string Text)> Notices { get; } = new();

        public SemaphoreSlim Proceed { get; } = new(0);

        public int ActiveChatCount => _chats.ActiveChatCount;

        public void Send(string senderId, string text, string channelId = "channels/c1") =>
            _chats.Enqueue(new TestMessage(senderId, text, new Channel { Id = channelId }));

        public Task StopAsync() => _chats.StopAsync();

        public async Task WaitEnteredAsync() =>
            Assert.True(await _entered.WaitAsync(WaitTimeout), "no turn started");

        public async Task RunBatchAsync(System.Threading.Channels.ChannelReader<TestMessage> queue, CancellationToken ct)
        {
            var messages = new List<TestMessage>();
            while (queue.TryRead(out var message))
                messages.Add(message);

            Turns.Enqueue((messages[0].SenderId, messages.Select(m => m.Text!).ToArray()));
            _entered.Release();
            await Proceed.WaitAsync(ct);
        }

        public Task NotifyBufferFullAsync(TestMessage message, CancellationToken ct)
        {
            Notices.Enqueue((message.SenderId, message.Text!));
            return Task.CompletedTask;
        }
    }
}
