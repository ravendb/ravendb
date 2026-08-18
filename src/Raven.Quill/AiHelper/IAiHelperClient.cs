using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper;

public interface IAiHelperClient
{
    Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct);

    Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct);

    /// <summary>Starts one AI assistant (chatbot) turn and hands back the upstream response for the
    /// caller to relay, granting consent and retrying first if the service asks for it. The response
    /// is the caller's to dispose.</summary>
    Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct);

    Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct);

    Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class;
}
