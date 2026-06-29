namespace Raven.AiAppliance.Contracts;

/// <summary>
/// One point of a usage series (<c>GET /api/usage?time=…&amp;app=…</c>) — the bucket
/// starting at <paramref name="Timestamp"/> (UTC). <paramref name="Conversations"/> =
/// conversations started, <paramref name="Messages"/> = user messages (agent turns),
/// <paramref name="Tokens"/> = summed token usage, all within the bucket.
/// </summary>
public sealed record UsagePoint(DateTime Timestamp, long Conversations, long Messages, long Tokens);
