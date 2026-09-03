using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper;

public interface IAiHelperClient
{
    Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct);

    Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct);

    /// <summary>Starts one AI assistant (chatbot) turn and hands back the upstream response for the
    /// caller to relay. The response is the caller's to dispose.</summary>
    Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct);

    /// <summary>Asks whether the license behind this appliance has already consented to sending data to
    /// the RavenDB AI service. Answers <see cref="AiHelperStatus.ConsentRequired"/> until it has.</summary>
    Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct);

    /// <summary>Records this appliance's consent to the AI service's terms. Only ever called for an
    /// operator who accepted them: nothing in Quill grants consent on their behalf.</summary>
    Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct);

    Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct);

    Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class;
}
