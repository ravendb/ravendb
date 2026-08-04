namespace Raven.Quill.Telegram;

/// Per-poller operational counters, in-memory by design: persisting them would write the channel doc on every
/// poll cycle for no durability value — a restart repopulates within one poll.
internal sealed class TelegramChannelHealth
{
    private long _lastSuccessfulPollTicks;
    private long _lastErrorAtTicks;
    private int _errorCount;
    private volatile string? _lastError;

    public void RecordSuccess(DateTime utcNow) =>
        Interlocked.Exchange(ref _lastSuccessfulPollTicks, utcNow.Ticks);

    /// The message must already be token-scrubbed; exception text can embed /bot{token}/ request urls.
    public void RecordError(DateTime utcNow, string scrubbedMessage)
    {
        Interlocked.Exchange(ref _lastErrorAtTicks, utcNow.Ticks);
        Interlocked.Increment(ref _errorCount);
        _lastError = scrubbedMessage;
    }

    public TelegramChannelHealthSnapshot Snapshot(bool isPolling)
    {
        var success = Interlocked.Read(ref _lastSuccessfulPollTicks);
        var error = Interlocked.Read(ref _lastErrorAtTicks);
        return new TelegramChannelHealthSnapshot(
            isPolling,
            success == 0 ? null : new DateTime(success, DateTimeKind.Utc),
            error == 0 ? null : new DateTime(error, DateTimeKind.Utc),
            Volatile.Read(ref _errorCount),
            _lastError);
    }
}

internal sealed record TelegramChannelHealthSnapshot(
    bool IsPolling,
    DateTime? LastSuccessfulPoll,
    DateTime? LastErrorAt,
    int ErrorCount,
    string? LastError);
