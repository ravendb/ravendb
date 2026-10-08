using System.Collections.Concurrent;
using System.Threading.Channels;
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

internal interface IChatTurns<in TMessage>
{
    Task RunTurnAsync(IReadOnlyList<TMessage> messages, CancellationToken ct);

    Task NotifyBufferFullAsync(TMessage message, CancellationToken ct);
}

internal interface IChannelChats
{
    Task StopAsync(CancellationToken cancellationToken);
}

internal sealed class ChannelChats<TMessage>(
    IChatTurns<TMessage> turns, IRavenLogger logger, int capacity, TimeSpan idleTimeout) : IChannelChats
    where TMessage : IChannelMessage
{
    private static readonly TimeSpan StopDrainTimeout = TimeSpan.FromSeconds(10);

    private readonly IChatTurns<TMessage> _turns = turns;
    private readonly int _capacity = capacity;
    private readonly TimeSpan _idleTimeout = idleTimeout;
    private readonly ConcurrentDictionary<string, Chat> _chats = new();
    private readonly CancellationTokenSource _stopping = new();

    internal int ActiveChatCount => _chats.Count;

    public void Enqueue(TMessage message)
    {
        var key = $"{message.Database}/{message.Channel.ShortId}/{message.SenderId}";

        while (true)
        {
            var chat = _chats.GetOrAdd(key, k => new Chat(this, k));

            if (chat.TryPost(message))
                return;

            if (chat.IsRetired)
            {
                OnChatRetired(key, chat);
                continue;
            }

            if (logger.IsWarnEnabled)
                logger.Warn($"sender {key} dropped a message: queue full");
            chat.NotifyBufferFullOnce(message);
            return;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await StopChatsAsync().WaitAsync(StopDrainTimeout, cancellationToken);
        }
        catch (TimeoutException)
        {
            if (logger.IsWarnEnabled)
                logger.Warn($"chats did not drain within {StopDrainTimeout}");
        }
    }

    private async Task RunBatchAsync(string key, IReadOnlyList<TMessage> messages)
    {
        var merged = new List<TMessage>();

        foreach (var message in messages)
        {
            if (message.RunsAlone == false)
            {
                if (string.IsNullOrWhiteSpace(message.Text) == false)
                    merged.Add(message);
                continue;
            }

            await FlushAsync(key, merged);
            await RunTurnSafeAsync(key, [message]);
        }

        await FlushAsync(key, merged);
    }

    private Task FlushAsync(string key, List<TMessage> merged)
    {
        if (merged.Count == 0)
            return Task.CompletedTask;

        var turn = merged.ToArray();
        merged.Clear();
        return RunTurnSafeAsync(key, turn);
    }

    private async Task RunTurnSafeAsync(string key, IReadOnlyList<TMessage> messages)
    {
        try
        {
            await _turns.RunTurnAsync(messages, _stopping.Token);
        }
        catch (Exception e)
        {
            OnTurnFailed(key, e);
        }
    }

    private async Task StopChatsAsync()
    {
        await _stopping.CancelAsync();
        await Task.WhenAll(_chats.Values.Select(c => c.Completion));
    }

    private void OnChatRetired(string key, Chat chat) =>
        _chats.TryRemove(new KeyValuePair<string, Chat>(key, chat));

    private void OnTurnFailed(string key, Exception e)
    {
        if (_stopping.IsCancellationRequested == false && logger.IsWarnEnabled)
            logger.Warn($"sender {key} turn failed: {e.Message}");
    }

    private sealed class Chat
    {
        private readonly ChannelChats<TMessage> _owner;
        private readonly string _key;
        private readonly Channel<TMessage> _queue;

        private int _bufferFullNotified;
        private int _retired;

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

        public bool IsRetired => Volatile.Read(ref _retired) == 1;

        public bool TryPost(TMessage message) => _queue.Writer.TryWrite(message);

        public void NotifyBufferFullOnce(TMessage message)
        {
            if (Interlocked.Exchange(ref _bufferFullNotified, 1) != 0)
                return;

            _ = _owner._turns.NotifyBufferFullAsync(message, _owner._stopping.Token);
        }

        private async Task RunAsync()
        {
            try
            {
                while (await WaitForMessageAsync())
                {
                    var messages = new List<TMessage>();
                    while (_queue.Reader.TryRead(out var message))
                        messages.Add(message);

                    await _owner.RunBatchAsync(_key, messages);

                    Interlocked.Exchange(ref _bufferFullNotified, 0);
                }
            }
            catch (OperationCanceledException) when (_owner._stopping.IsCancellationRequested)
            {
            }
        }

        private async Task<bool> WaitForMessageAsync()
        {
            var wait = _queue.Reader.WaitToReadAsync(_owner._stopping.Token).AsTask();
            try
            {
                return await wait.WaitAsync(_owner._idleTimeout);
            }
            catch (TimeoutException)
            {
                Retire();
                return await wait;
            }
        }

        private void Retire()
        {
            Interlocked.Exchange(ref _retired, 1);
            _queue.Writer.TryComplete();
            _owner.OnChatRetired(_key, this);
        }
    }
}
