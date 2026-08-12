namespace Raven.Quill.Telegram;

internal sealed class TelegramChannelHealth
{
    private long _lastSuccessfulPollTicks;
    private long _lastErrorAtTicks;
    private int _errorCount;
    private volatile string? _lastError;

    public void RecordSuccess(DateTime utcNow) =>
        Interlocked.Exchange(ref _lastSuccessfulPollTicks, utcNow.Ticks);

    public void RecordError(DateTime utcNow, string message)
    {
        Interlocked.Exchange(ref _lastErrorAtTicks, utcNow.Ticks);
        Interlocked.Increment(ref _errorCount);
        _lastError = message;
    }

    public TelegramChannelHealthSnapshot Snapshot()
    {
        var success = Interlocked.Read(ref _lastSuccessfulPollTicks);
        var error = Interlocked.Read(ref _lastErrorAtTicks);
        return new TelegramChannelHealthSnapshot(
            IsPolling: true,
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
