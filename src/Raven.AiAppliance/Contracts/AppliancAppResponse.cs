namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Enriched app summary for the global Dashboard apps table — the prototype's
/// <c>AppliancApp</c>. Built by fan-out over each app DB (counts + CDC source +
/// derived status); served from <c>GET /api/dashboard/apps</c> (the wizard-owned
/// <c>/api/apps</c> keeps its minimal shape). <c>writesPerMonth</c> is null (no write
/// counter yet — gap #4); <c>status</c>/<c>statusSubtitle</c> are derived.
/// </summary>
public sealed record AppliancAppResponse(
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

/// <param name="Type">Source DB family (PostgreSQL/MySQL/SQL Server/Oracle), or "" if no CDC.</param>
/// <param name="ConnectionString">Always "" — the real connection string is a secret and never exposed.</param>
public sealed record AppSource(string Type, string ConnectionString);
