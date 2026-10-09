using System.Collections.Concurrent;
using System.Threading.Channels;
using Raven.Quill.Hosting;
using Sparrow.Logging;

namespace Raven.Quill.Channels;

internal interface IChannelMessage
{
    string Database { get; }

    Channel Channel { get; }

    string SenderId { get; }

    string? Text { get; }

    bool RunsAlone { get; }

    bool IsUnsupported { get; }
}

internal interface IChatTurns<TMessage>
{
    Task RunBatchAsync(ChannelReader<TMessage> queue, CancellationToken ct);

    Task NotifyBufferFullAsync(TMessage message, CancellationToken ct);
}

internal sealed class ChannelChats<TMessage>(IChatTurns<TMessage> turns, ApplianceOptions options, IRavenLogger logger)
    where TMessage : IChannelMessage
{
    private readonly int _capacity = options.ChannelSenderQueueCapacity;
    private readonly TimeSpan _idleTimeout = options.ChannelSenderIdleTimeout;
    private readonly ConcurrentDictionary<string, Chat> _chats = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly IRavenLogger _logger = logger;

    internal int ActiveChatCount => _chats.Count;

    public void Enqueue(TMessage message)
    {
        var key = $"{message.Database}/{message.Channel.ShortId}/{message.SenderId}";

        while (_stopping.IsCancellationRequested == false)
        {
            var chat = _chats.GetOrAdd(key, k => new Chat(this, k));

            if (chat.TryPost(message))
                return;

            if (chat.Completion.IsCompleted)
                continue;

            if (chat.TryMarkBufferFull())
            {
                if (_logger.IsWarnEnabled)
                    _logger.Warn($"sender {key} is dropping messages: queue full");
                _ = turns.NotifyBufferFullAsync(message, _stopping.Token);
            }
            return;
        }
    }

    public async Task StopAsync()
    {
        await _stopping.CancelAsync();
        await Task.WhenAll(_chats.Values.Select(c => c.Completion))
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private Task RunBatchAsync(ChannelReader<TMessage> queue) => turns.RunBatchAsync(queue, _stopping.Token);

    private sealed class Chat
    {
        private readonly ChannelChats<TMessage> _owner;
        private readonly string _key;
        private readonly Channel<TMessage> _queue;

        private int _bufferFullNotified;

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

            Completion = Task.Run(RunAsync, CancellationToken.None);
        }

        public Task Completion { get; }

        public bool TryPost(TMessage message) => _queue.Writer.TryWrite(message);

        public bool TryMarkBufferFull() => Interlocked.Exchange(ref _bufferFullNotified, 1) == 0;

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

                    await _owner.RunBatchAsync(_queue.Reader);

                    Interlocked.Exchange(ref _bufferFullNotified, 0);
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                if (_owner._logger.IsWarnEnabled)
                    _owner._logger.Warn($"sender {_key} chat stopped: {e.Message}");
                throw;
            }
            finally
            {
                _owner._chats.TryRemove(new(_key, this));
                _queue.Writer.TryComplete();

                while (_queue.Reader.TryRead(out var leftover))
                    _owner.Enqueue(leftover);
            }
        }
    }
}
