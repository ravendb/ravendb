using System.Collections.Concurrent;

namespace Raven.Quill.Discord;

internal sealed class DiscordHealthRegistry
{
    internal static readonly TimeSpan TokenCheckMaxAge = TimeSpan.FromMinutes(5);

    internal sealed record Snapshot(
        bool GatewayConnected,
        DateTime? LastConnectedAt,
        string? LastGatewayError,
        DateTime? LastInboundAt,
        DateTime? LastSendErrorAt,
        string? LastSendError);

    private sealed class Entry
    {
        public bool GatewayConnected;
        public DateTime? LastConnectedAt;
        public string? LastGatewayError;
        public DateTime? LastInboundAt;
        public DateTime? LastSendErrorAt;
        public string? LastSendError;
        public DateTime? TokenCheckedAt;
        public bool? TokenValid;
        public string? TokenError;
    }

    private readonly ConcurrentDictionary<(string Database, string ChannelId), Entry> _entries = new();

    public void RecordGatewayConnected(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            entry.GatewayConnected = true;
            entry.LastConnectedAt = DateTime.UtcNow;
            entry.LastGatewayError = null;
        }
    }

    public void RecordGatewayDisconnected(string database, string channelId, string? error)
    {
        if (error is null)
        {
            TryUpdate(database, channelId, entry => entry.GatewayConnected = false);
            return;
        }

        var entry = EntryFor(database, channelId);
        lock (entry)
        {
            entry.GatewayConnected = false;
            entry.LastGatewayError = error;
        }
    }

    public void RecordInbound(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            entry.LastInboundAt = DateTime.UtcNow;
    }

    public void RecordSendError(string database, string channelId, string error) =>
        TryUpdate(database, channelId, entry =>
        {
            entry.LastSendErrorAt = DateTime.UtcNow;
            entry.LastSendError = error;
        });

    public void RecordSendSuccess(string database, string channelId) =>
        TryUpdate(database, channelId, entry =>
        {
            entry.LastSendErrorAt = null;
            entry.LastSendError = null;
        });

    public Snapshot SnapshotFor(string database, string channelId)
    {
        var entry = EntryFor(database, channelId);
        lock (entry)
            return new Snapshot(
                entry.GatewayConnected, entry.LastConnectedAt, entry.LastGatewayError,
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
