namespace Raven.AiAppliance.Wizard;

using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

/// <summary>
/// Result of <c>POST /api/setup/connect</c> — now a plain reachability outcome from
/// the SQL <c>test-connection</c> probe. CDC-readiness fields
/// (<c>HasPermissionToSetup</c>, <c>Warnings</c>) moved to the Discover response,
/// which is where verification lives after the server merged verify into
/// <c>/admin/cdc-sink/schema</c>.
/// </summary>
public sealed class ConnectResult
{
    [SetsRequiredMembers]
    public ConnectResult()
    {
    }

    [JsonRequired]
    public required bool Success { get; set; }

    [JsonRequired]
    public required List<string> Errors { get; set; } = new();
}
