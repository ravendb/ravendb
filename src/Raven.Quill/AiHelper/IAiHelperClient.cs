using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper;

/// <summary>
/// Abstraction over the AI-Helper config-generation calls. The single implementation
/// (<see cref="AiHelperInternalClient"/>) proxies through the bundled RavenDB server's
/// <c>/assistant/assist</c> handler, which injects the license + cert and forwards to the internal
/// AI service on api.ravendb.net.
/// </summary>
public interface IAiHelperClient
{
    Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct);

    Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct);

    Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct);

    Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class;
}
