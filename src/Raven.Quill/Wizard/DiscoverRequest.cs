namespace Raven.Quill.Wizard;

public sealed record DiscoverRequest(
    string Provider,
    string ConnectionString,
    string[]? Schemas = null,
    string Slug = "");
