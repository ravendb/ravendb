namespace Raven.Quill.Contracts;

public sealed record UsageResponse(List<UsagePoint> Points, List<AppWrites> WritesByApp);

public sealed record UsagePoint(DateTime Timestamp, long Conversations, long Messages, long Tokens, long Writes);

public sealed record AppWrites(string Slug, long Writes);
