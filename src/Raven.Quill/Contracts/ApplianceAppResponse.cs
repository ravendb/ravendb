namespace Raven.Quill.Contracts;

public sealed record ApplianceAppResponse(
    string Id,
    string Name,
    string Slug,
    string Status,
    AppSource Source,
    int TablesCount,
    long DocumentsCount,
    int CapabilitiesCount,
    int ChannelsCount,
    int AdaptersCount,
    int AgentsCount,
    long? WritesPerMonth,
    string? ChannelsLabel,
    string? StatusSubtitle,
    DateTime CreatedAt,
    DateTime UpdatedAt);

// ConnectionString is always ""; the real one is a secret
public sealed record AppSource(string Type, string ConnectionString);
