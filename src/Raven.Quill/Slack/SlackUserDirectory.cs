using System.Collections.Concurrent;

namespace Raven.Quill.Slack;

internal sealed class SlackUserDirectory
{
    internal static readonly TimeSpan EntryMaxAge = TimeSpan.FromMinutes(10);

    private const int MaxEntries = 4096;

    private readonly ConcurrentDictionary<(string TeamId, string UserId), (SlackUserInfo Info, DateTime CachedAt)> _entries = new();

    public async Task<SlackUserInfo> GetAsync(
        ISlackClient slack, string botToken, string teamId, string userId, CancellationToken ct)
    {
        var key = (teamId, userId);
        if (_entries.TryGetValue(key, out var cached) && DateTime.UtcNow - cached.CachedAt < EntryMaxAge)
            return cached.Info;

        var info = await slack.UserInfoAsync(botToken, userId, ct);

        // a profile with no email is not cached, so filling one in takes effect on the next message
        if (info.Email is null)
            return info;

        if (_entries.Count >= MaxEntries)
        {
            var now = DateTime.UtcNow;
            foreach (var (staleKey, entry) in _entries)
            {
                if (now - entry.CachedAt >= EntryMaxAge)
                    _entries.TryRemove(staleKey, out _);
            }

            if (_entries.Count >= MaxEntries)
                _entries.Clear();
        }

        _entries[key] = (info, DateTime.UtcNow);
        return info;
    }
}
