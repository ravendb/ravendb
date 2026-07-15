using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper;

public interface IAiHelperClient
{
    Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct);

    Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct);

    Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct);

    Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class;
}
