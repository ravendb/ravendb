namespace Raven.Quill.Contracts;

/// <summary>
/// One point of a usage series (<c>GET /api/usage?time=…&amp;app=…</c>) — the bucket
/// starting at <paramref name="Timestamp"/> (UTC). <paramref name="Conversations"/> =
/// conversations started, <paramref name="Messages"/> = user messages (agent turns),
/// <paramref name="Tokens"/> = summed token usage, <paramref name="Writes"/> = CDC writes,
/// all within the bucket. Writes are a deterministic per-app mock until the real per-DB
/// write counter lands (RavenDB-26780).
/// </summary>
public sealed record UsagePoint(DateTime Timestamp, long Conversations, long Messages, long Tokens, long Writes);
