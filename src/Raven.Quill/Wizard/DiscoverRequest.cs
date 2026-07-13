namespace Raven.Quill.Wizard;

/// <summary>
/// Body of <c>POST /api/setup/discover</c>. Same connection inputs as
/// <see cref="ConnectRequest"/> plus an optional list of schemas to enumerate.
/// When <see cref="Schemas"/> is null or empty the source database's default
/// schema is discovered (mirrors the Studio CDC Sink schema explorer).
/// </summary>
public sealed record DiscoverRequest(
    string Provider,
    string ConnectionString,
    string[]? Schemas = null);
