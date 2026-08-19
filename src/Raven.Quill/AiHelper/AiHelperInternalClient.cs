using System.Net;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Microsoft.Extensions.Logging;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Quill.AiHelper;

public sealed class AiHelperInternalClient(
    HttpClient httpClient,
    IDocumentStore store,
    ILogger<AiHelperInternalClient> logger) : IAiHelperClient
{
    private const string AssistPath = "/assistant/assist";

    private const string CheckConsentPath = "/assistant/check-consent";

    private const string GiveConsentPath = "/assistant/give-consent";

    public async Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct)
    {
        var request = new SuggestCdcApiRequest
        {
            Schema = schema,
            Samples = samples,
            Prompt = prompt,
        };

        var (transport, content) = await SendAsync(AssistPath, "POST", request, ct);
        if (transport != AiHelperStatus.Success)
            return new SuggestCdcInternalResult(transport, Configuration: null, [], 0, 0);

        var wire = await DeserializeAsync<SuggestCdcApiResponse>(content, ct);
        if (wire is null)
            return new SuggestCdcInternalResult(AiHelperStatus.InternalError, Configuration: null, [], 0, 0);

        var status = ParseStatus(wire.Status);
        return new SuggestCdcInternalResult(
            status,
            status == AiHelperStatus.Success ? wire.Configuration : null,
            wire.Rationale ?? [],
            wire.InputTokenCount,
            wire.OutputTokenCount);
    }

    public async Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct)
    {
        var request = new SuggestAiAgentApiRequest
        {
            CdcConfig = cdcConfig,
            CollectionsSample = collectionsSample,
            Mode = mode,
            Prompt = prompt,
        };

        var (transport, content) = await SendAsync(AssistPath, "POST", request, ct);
        if (transport != AiHelperStatus.Success)
            return new SuggestAiAgentInternalResult(transport, [], [], 0, 0);

        var wire = await DeserializeAsync<SuggestAiAgentApiResponse>(content, ct);
        if (wire is null)
            return new SuggestAiAgentInternalResult(AiHelperStatus.InternalError, [], [], 0, 0);

        var status = ParseStatus(wire.Status);
        return new SuggestAiAgentInternalResult(
            status,
            status == AiHelperStatus.Success ? wire.Configurations ?? [] : [],
            wire.Rationale ?? [],
            wire.InputTokenCount,
            wire.OutputTokenCount);
    }

    public async Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct)
    {
        var request = new ChatbotApiRequest
        {
            Message = message,
            ConversationId = conversationId,
            RavenVersion = ServerVersion.Build,
        };

        using var content = new StringContent(SerializeRequest(request), Encoding.UTF8, "application/json");
        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, AssistPath) { Content = content };
        return await httpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, ct);
    }

    public async Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct)
    {
        try
        {
            using var content = new StringContent(SerializeRequest(request), Encoding.UTF8, "application/json");
            using var response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(method), path) { Content = content }, ct);
            var text = await response.Content.ReadAsStringAsync(ct);

            if (response.IsSuccessStatusCode)
                return (AiHelperStatus.Success, text);

            var status = response.StatusCode switch
            {
                HttpStatusCode.Unauthorized => await ClassifyUnauthorizedAsync(text, ct),
                HttpStatusCode.TooManyRequests => AiHelperStatus.OutOfTokens,
                _ => AiHelperStatus.InternalError,
            };

            logger.Log(
                status == AiHelperStatus.ConsentRequired ? LogLevel.Information : LogLevel.Warning,
                "AI Helper {Path} failed: upstream {Code}, mapped {Status}. Body: {Body}",
                path, (int)response.StatusCode, status, TruncateForLog(text));
            return (status, text);
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            logger.LogWarning(e, "AI Helper {Path} failed (transport).", path);
            return (AiHelperStatus.InternalError, string.Empty);
        }
    }

    public Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct) =>
        SendConsentRequestAsync(CheckConsentPath, HttpMethod.Get, ct);

    public Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct) =>
        SendConsentRequestAsync(GiveConsentPath, HttpMethod.Post, ct);

    // Both consent calls answer with a { Status } body — 200 once the service is satisfied, 401 while
    // it still wants consent or rejects the license — so the status is read out of the body either way.
    private async Task<AiHelperStatus> SendConsentRequestAsync(string path, HttpMethod method, CancellationToken ct)
    {
        try
        {
            using var httpRequest = new HttpRequestMessage(method, path);
            if (method == HttpMethod.Post)
                httpRequest.Content = new StringContent("{}", Encoding.UTF8, "application/json");

            using var response = await httpClient.SendAsync(httpRequest, ct);
            var text = await response.Content.ReadAsStringAsync(ct);

            var status = response.IsSuccessStatusCode
                ? ParseStatus((await DeserializeAsync<StatusOnly>(text, ct))?.Status)
                : response.StatusCode == HttpStatusCode.Unauthorized
                    ? await ClassifyUnauthorizedAsync(text, ct)
                    : AiHelperStatus.InternalError;

            if (status != AiHelperStatus.Success)
            {
                logger.Log(
                    status == AiHelperStatus.ConsentRequired ? LogLevel.Information : LogLevel.Warning,
                    "AI Helper {Path}: upstream {Code}, mapped {Status}. Body: {Body}",
                    path, (int)response.StatusCode, status, TruncateForLog(text));
            }

            return status;
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            logger.LogWarning(e, "AI Helper {Path} failed (transport).", path);
            return AiHelperStatus.InternalError;
        }
    }

    private async Task<AiHelperStatus> ClassifyUnauthorizedAsync(string body, CancellationToken ct)
    {
        var wire = await DeserializeAsync<StatusOnly>(body, ct);
        return ParseStatus(wire?.Status) == AiHelperStatus.ConsentRequired
            ? AiHelperStatus.ConsentRequired
            : AiHelperStatus.InvalidCredentials;
    }

    // never log the serialized request: it carries license keys
    private string SerializeRequest(object request)
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        return store.Conventions.Serialization.DefaultConverter.ToBlittable(request, ctx).ToString();
    }

    public async Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class
    {
        try
        {
            using var ctx = JsonOperationContext.ShortTermSingleUse();
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            var blittable = await ctx.ReadForMemoryAsync(stream, "ai-helper-response", ct);
            return store.Conventions.Serialization.DefaultConverter.FromBlittable<T>(blittable);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // Deserialization of a RavenDB response failed
            return null!;
        }
    }

    private static AiHelperStatus ParseStatus(string? status) =>
        Enum.TryParse<AiHelperStatus>(status, ignoreCase: true, out var parsed)
            ? parsed
            : AiHelperStatus.InternalError;

    private const int MaxLoggedBodyLength = 2000;

    private static string TruncateForLog(string body) =>
        body.Length <= MaxLoggedBodyLength ? body : body[..MaxLoggedBodyLength] + "…(truncated)";

    private sealed class StatusOnly
    {
        public string? Status { get; set; }
    }

    private sealed class ChatbotApiRequest
    {
        public string OperationType { get; set; } = "Chatbot";
        public string Message { get; set; } = null!;
        public string? ConversationId { get; set; }
        public int RavenVersion { get; set; }
    }

    private sealed class SuggestCdcApiRequest
    {
        public string OperationType { get; set; } = "CdcConfigSetup";
        public object? Schema { get; set; }
        public object? Samples { get; set; }
        public string Prompt { get; set; } = null!;
    }

    private sealed class SuggestAiAgentApiRequest
    {
        public string OperationType { get; set; } = "CdcBasedAgentConfigSetup";
        public CdcSinkConfiguration CdcConfig { get; set; } = null!;
        public object? CollectionsSample { get; set; }
        public string Mode { get; set; } = null!;
        public string? Prompt { get; set; }
    }

    private sealed class SuggestCdcApiResponse
    {
        public string? Status { get; set; }
        public CdcSinkConfiguration? Configuration { get; set; }
        public string[]? Rationale { get; set; }
        public int InputTokenCount { get; set; }
        public int OutputTokenCount { get; set; }
    }

    private sealed class SuggestAiAgentApiResponse
    {
        public string? Status { get; set; }
        public AiAgentConfiguration[]? Configurations { get; set; }
        public string[]? Rationale { get; set; }
        public int InputTokenCount { get; set; }
        public int OutputTokenCount { get; set; }
    }
}

public sealed record SuggestCdcInternalResult(
    AiHelperStatus Status,
    CdcSinkConfiguration? Configuration,
    IReadOnlyList<string> Rationale,
    int InputTokenCount,
    int OutputTokenCount);

public sealed record SuggestAiAgentInternalResult(
    AiHelperStatus Status,
    IReadOnlyList<AiAgentConfiguration> Configurations,
    IReadOnlyList<string> Rationale,
    int InputTokenCount,
    int OutputTokenCount);
