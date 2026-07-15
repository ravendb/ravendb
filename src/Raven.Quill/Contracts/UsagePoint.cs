namespace Raven.Quill.Contracts;

public sealed record UsagePoint(DateTime Timestamp, long Conversations, long Messages, long Tokens, long Writes);
