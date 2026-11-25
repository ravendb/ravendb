using System.Collections.Concurrent;
using System.Threading.Channels;
using Raven.Quill.Hosting;
using Sparrow.Logging;

namespace Raven.Quill.Channels;

internal interface IChannelMessage
{
    string SenderId { get; }

    string? Text { get; }

    ChannelMessageKind Kind { get; }
}

internal enum ChannelMessageKind
{
    Prompt,
    Command,
    Unsupported,
}

internal interface IChatTurns<TMessage>
{
    Task RunBatchAsync(ChannelReader<TMessage> queue, CancellationToken ct);

    Task NotifyBufferFullAsync(TMessage message, CancellationToken ct);
}

// By design: no redelivery dedupe, StopAsync drops queued turns, a retiring chat may briefly overlap its successor.
internal sealed class ChannelChats<TMessage>(
    Channel channel, IChatTurns<TMessage> turns, ApplianceOptions options, IRavenLogger logger)
    where TMessage : IChannelMessage
{
    private readonly string _channelId = channel.ShortId;
    private readonly int _capacity = options.ChannelSenderQueueCapacity;
    private readonly TimeSpan _idleTimeout = options.ChannelSenderIdleTimeout;
    private readonly ConcurrentDictionary<string, Chat> _chats = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly IRavenLogger _logger = logger;

    internal int ActiveChatCount => _chats.Count;

    public void Enqueue(TMessage message)
    {
        var key = message.SenderId;

        while (_stopping.IsCancellationRequested == false)
        {
            var chat = _chats.GetOrAdd(key, k => new Chat(this, k));

            switch (chat.TryPost(message))
            {
                case ChatPostResult.Retired:
                    _chats.TryRemove(new(key, chat));
                    continue;
                case ChatPostResult.Overloaded:
                    if (_logger.IsWarnEnabled)
                        _logger.Warn($"channel {_channelId} sender {key} is dropping messages: queue full");
                    _ = turns.NotifyBufferFullAsync(message, _stopping.Token);
                    break;
            }

            return;
        }
    }

    public async Task StopAsync()
    {
        await _stopping.CancelAsync();
        await Task.WhenAll(_chats.Values.Select(c => c.Worker))
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private Task RunBatchAsync(ChannelReader<TMessage> queue) => turns.RunBatchAsync(queue, _stopping.Token);

    private enum ChatPostResult
    {
        Posted,
        Overloaded,
        Dropped,
        Retired,
    }

    private sealed class Chat
    {
        private readonly ChannelChats<TMessage> _owner;
        private readonly string _key;
        private readonly Channel<TMessage> _queue;
        private volatile bool _closed;
        private int _notified;

        public Chat(ChannelChats<TMessage> owner, string key)
        {
            _owner = owner;
            _key = key;
            _queue = System.Threading.Channels.Channel.CreateBounded<TMessage>(
                new BoundedChannelOptions(owner._capacity)
                {
                    SingleReader = true,
                    FullMode = BoundedChannelFullMode.Wait,
                });

            Worker = Task.Run(RunAsync, CancellationToken.None);
        }

        public Task Worker { get; }

        public ChatPostResult TryPost(TMessage message)
        {
            if (_queue.Writer.TryWrite(message))
                return ChatPostResult.Posted;

            if (_closed)
                return ChatPostResult.Retired;

            return Interlocked.Exchange(ref _notified, 1) == 0 ? ChatPostResult.Overloaded : ChatPostResult.Dropped;
        }

        private async Task RunAsync()
        {
            try
            {
                while (true)
                {
                    using (var idle = CancellationTokenSource.CreateLinkedTokenSource(_owner._stopping.Token))
                    {
                        idle.CancelAfter(_owner._idleTimeout);
                        await _queue.Reader.WaitToReadAsync(idle.Token);
                    }

                    await RunBatchSafelyAsync();
                }
            }
            finally
            {
                _closed = true;
                _queue.Writer.TryComplete();

                if (_owner._stopping.IsCancellationRequested == false)
                    await RunBatchSafelyAsync();

                _owner._chats.TryRemove(new(_key, this));
            }
        }

        private async Task RunBatchSafelyAsync()
        {
            try
            {
                await _owner.RunBatchAsync(_queue.Reader);
            }
            catch (Exception e)
            {
                if (_owner._stopping.IsCancellationRequested)
                    return;

                if (_owner._logger.IsWarnEnabled)
                    _owner._logger.Warn($"channel {_owner._channelId} sender {_key} turn batch failed: {e.Message}");
            }
            finally
            {
                Interlocked.Exchange(ref _notified, 0);
            }
        }
    }
}
