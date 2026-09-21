using System.Collections.Concurrent;

namespace Raven.Quill.Slack;

internal sealed class SlackHealthRegistry
{
    internal static readonly TimeSpan TokenCheckMaxAge = TimeSpan.FromMinutes(5);

    internal sealed record Snapshot(
        bool SocketConnected,
        DateTime? LastConnectedAt,
        string? LastSocketError,
        DateTime? LastInboundAt,
        DateTime? LastSendErrorAt,
        string? LastSendError);

    private sealed class Entry
    {
        public bool SocketConnected;
        public DateTime? LastConnectedAt;
        public string? LastSocketError;
        public DateTime? LastInboundAt;
        public DateTime? LastSendErrorAt;
        public string? LastSendError;
        public DateTime? TokenCheckedAt;
        public bool? TokenValid;
        public string? TokenError;
    }

    private readonly ConcurrentDictionary<(string Database, string ChannelId), Entry> _entries = new();

    public void RecordSocketConnected(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            entry.SocketConnected = true;
            entry.LastConnectedAt = DateTime.UtcNow;
            entry.LastSocketError = null;
        }
    }

    public void RecordSocketDisconnected(string database, string channelId, string? error)
    {
        if (error is null)
        {
            TryUpdate(database, channelId, entry => entry.SocketConnected = false);
            return;
        }

        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            entry.SocketConnected = false;
            entry.LastSocketError = error;
        }
    }

    public void RecordSocketStopped(string database, string channelId) =>
        TryUpdate(database, channelId, entry =>
        {
            entry.SocketConnected = false;
            entry.LastSocketError = null;
        });

    public void RecordInbound(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            entry.LastInboundAt = DateTime.UtcNow;
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
            return new Snapshot(
                entry.SocketConnected, entry.LastConnectedAt, entry.LastSocketError,
                entry.LastInboundAt, entry.LastSendErrorAt, entry.LastSendError);
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

    private void TryUpdate(string database, string channelId, Action<Entry> update)
    {
        if (_entries.TryGetValue((database, channelId), out var entry) == false)
            return;

        lock (entry)
            update(entry);
    }

    private Entry EntryFor(string database, string channelId) =>
        _entries.GetOrAdd((database, channelId), static _ => new Entry());
}
