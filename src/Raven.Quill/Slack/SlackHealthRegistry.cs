using System.Collections.Concurrent;

namespace Raven.Quill.Slack;

internal sealed class SlackHealthRegistry
{
    internal static readonly TimeSpan TokenCheckMaxAge = TimeSpan.FromMinutes(5);

    internal sealed record Snapshot(
        DateTime? LastInboundAt,
        DateTime? LastSignatureFailureAt,
        DateTime? LastSendErrorAt,
        string? LastSendError);

    private sealed class Entry
    {
        public DateTime? LastInboundAt;
        public DateTime? LastSignatureFailureAt;
        public DateTime? LastSendErrorAt;
        public string? LastSendError;
        public DateTime? TokenCheckedAt;
        public bool? TokenValid;
        public string? TokenError;
    }

    private readonly ConcurrentDictionary<(string Database, string ChannelId), Entry> _entries = new();

    public void RecordInbound(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            entry.LastInboundAt = DateTime.UtcNow;
    }

    public void RecordSignatureFailure(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            entry.LastSignatureFailureAt = DateTime.UtcNow;
    }

    public void RecordSendError(string database, string channelId, string error)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            entry.LastSendErrorAt = DateTime.UtcNow;
            entry.LastSendError = error;
        }
    }

    public Snapshot SnapshotFor(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            return new Snapshot(entry.LastInboundAt, entry.LastSignatureFailureAt, entry.LastSendErrorAt, entry.LastSendError);
    }

    public bool TryGetFreshTokenCheck(string database, string channelId, out bool? valid, out string? error)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            if (entry.TokenCheckedAt is { } at && DateTime.UtcNow - at < TokenCheckMaxAge)
            {
                valid = entry.TokenValid;
                error = entry.TokenError;
                return true;
            }
        }

        valid = null;
        error = null;
        return false;
    }

    public void StoreTokenCheck(string database, string channelId, bool? valid, string? error)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            entry.TokenCheckedAt = DateTime.UtcNow;
            entry.TokenValid = valid;
            entry.TokenError = error;
        }
    }

    public void InvalidateTokenCheck(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            entry.TokenCheckedAt = null;
    }

    public void Remove(string database, string channelId) =>
        _entries.TryRemove((database, channelId), out _);

    public void RemoveDatabase(string database)
    {
        foreach (var key in _entries.Keys)
        {
            if (key.Database == database)
                _entries.TryRemove(key, out _);
        }
    }

    private Entry EntryFor(string database, string channelId) =>
        _entries.GetOrAdd((database, channelId), static _ => new Entry());
}
