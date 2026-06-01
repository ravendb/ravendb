using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Abstraction over the AI-Helper config-generation calls. The production implementation
/// (<see cref="AiHelperInternalClient"/>) forwards to the internal AI service on api.ravendb.net.
/// <see cref="MockAiHelperClient"/> returns canned Northwind sample data in demo mode
/// (the setup-package zip is mounted) when the internal service is unavailable.
/// </summary>
public interface IAiHelperClient
{
    Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct);

    Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct);
}
