using System.Net;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Microsoft.Extensions.Logging;
using Sparrow.Json;

namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Typed client for the AI-Helper endpoints, proxied through the bundled RavenDB server's
/// <c>/assistant/assist</c> handler. That handler injects the license + client-cert thumbprint from
/// its own ServerStore and forwards to api.ravendb.net, so the appliance never reaches the external
/// API directly. Maps transport outcomes (401/429/non-2xx) to <see cref="AiHelperStatus"/>; on a
/// first-use <c>ConsentRequired</c> it signs consent via <c>/assistant/give-consent</c> and retries once.
/// Request/response payloads are serialized through <c>store.Conventions.Serialization</c>, keeping
/// the wire shape byte-identical to the RavenDB-based internal service.
/// Registered as a typed <c>HttpClient</c> whose <c>BaseAddress</c> is the bundled RavenDB node and
/// whose handler presents the admin client cert.
/// </summary>
public sealed class AiHelperInternalClient(
    HttpClient httpClient,
    IDocumentStore store,
    ILogger<AiHelperInternalClient> logger) : IAiHelperClient
{
    // Proxy entrypoint on the bundled RavenDB server; the operation is selected by
    // OperationType on each request DTO (CdcConfigSetup / CdcBasedAgentConfigSetup).
    private const string AssistPath = "/assistant/assist";

    // Sibling proxy that signs AI consent for the server's license + the calling cert thumbprint.
    // The real /assist gates on a consent document for that pair; we sign on first use, then retry.
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

        var (transport, content) = await SendWithConsentRetryAsync(AssistPath, request, ct);
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

        var (transport, content) = await SendWithConsentRetryAsync(AssistPath, request, ct);
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

    /// Sends the assist request; on the first ConsentRequired (no consent doc yet for this
    /// license + cert pair) signs consent via the proxy and retries once. give-consent also
    /// verifies the license, so a failure there surfaces the real credential problem instead.
    private async Task<(AiHelperStatus Transport, string Content)> SendWithConsentRetryAsync(string path, object request, CancellationToken ct)
    {
        var result = await SendAsync(path, request, ct);
        if (result.Transport != AiHelperStatus.ConsentRequired)
            return result;

        var consent = await GiveConsentAsync(ct);
        if (consent != AiHelperStatus.Success)
            return (consent, string.Empty);

        return await SendAsync(path, request, ct);
    }

    private async Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, object request, CancellationToken ct)
    {
        try
        {
            using var content = new StringContent(SerializeRequest(request), Encoding.UTF8, "application/json");
            using var response = await httpClient.PostAsync(path, content, ct);
            var text = await response.Content.ReadAsStringAsync(ct);

            if (response.IsSuccessStatusCode)
                return (AiHelperStatus.Success, text);

            // A 401 carries the real reason in the forwarded body Status (ConsentRequired vs
            // InvalidCredentials); the HTTP code alone can't tell them apart.
            var status = response.StatusCode switch
            {
                HttpStatusCode.Unauthorized => await ClassifyUnauthorizedAsync(text, ct),
                HttpStatusCode.TooManyRequests => AiHelperStatus.OutOfTokens,
                _ => AiHelperStatus.InternalError,
            };

            // The response body is the internal service's status envelope (no secrets), so log it to
            // keep this diagnosable. The request body carries license keys — never log that.
            logger.LogWarning("AI Helper {Path} failed: upstream {Code}, mapped {Status}. Body: {Body}",
                path, (int)response.StatusCode, status, text);
            return (status, text);
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            // DNS/TLS/socket failure or an HttpClient timeout (not caller cancellation) maps to
            // InternalError; caller cancellation propagates.
            // `using var response` inside the try guarantees disposal on every path.
            logger.LogWarning(e, "AI Helper {Path} failed (transport).", path);
            return (AiHelperStatus.InternalError, string.Empty);
        }
    }

    /// Signs AI consent for the server's license + the calling cert thumbprint via the proxy (which
    /// injects both server-side, so an empty body is correct). A 401 here means give-consent's own
    /// license check rejected the license — surfaced as InvalidCredentials, not a consent problem.
    private async Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct)
    {
        try
        {
            using var content = new StringContent("{}", Encoding.UTF8, "application/json");
            using var response = await httpClient.PostAsync(GiveConsentPath, content, ct);
            if (response.IsSuccessStatusCode)
                return AiHelperStatus.Success;

            var text = await response.Content.ReadAsStringAsync(ct);
            var status = response.StatusCode == HttpStatusCode.Unauthorized
                ? AiHelperStatus.InvalidCredentials
                : AiHelperStatus.InternalError;
            logger.LogWarning("AI Helper give-consent failed: upstream {Code}, mapped {Status}. Body: {Body}",
                (int)response.StatusCode, status, text);
            return status;
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            logger.LogWarning(e, "AI Helper give-consent failed (transport).");
            return AiHelperStatus.InternalError;
        }
    }

    /// The proxy forwards the internal service's 401 body verbatim; its Status distinguishes a
    /// missing/stale consent doc (ConsentRequired) from a rejected license (InvalidCredentials).
    private async Task<AiHelperStatus> ClassifyUnauthorizedAsync(string body, CancellationToken ct)
    {
        var wire = await DeserializeAsync<StatusOnly>(body, ct);
        return ParseStatus(wire?.Status) == AiHelperStatus.ConsentRequired
            ? AiHelperStatus.ConsentRequired
            : AiHelperStatus.InvalidCredentials;
    }

    private string SerializeRequest(object request)
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        return store.Conventions.Serialization.DefaultConverter.ToBlittable(request, ctx).ToString();
    }

    private async Task<T?> DeserializeAsync<T>(string json, CancellationToken ct) where T : class
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
            return null;
        }
    }

    private static AiHelperStatus ParseStatus(string? status) =>
        Enum.TryParse<AiHelperStatus>(status, ignoreCase: true, out var parsed)
            ? parsed
            : AiHelperStatus.InternalError;

    /// Minimal projection for reading just the Status field from a non-2xx response envelope.
    private sealed class StatusOnly
    {
        public string? Status { get; set; }
    }

    private sealed class SuggestCdcApiRequest
    {
        // OperationType routes the consolidated assist endpoint (sent as the exact enum name).
        // License + CertificateThumbprint are injected by the RavenDB /assistant/assist proxy.
        public string OperationType { get; set; } = "CdcConfigSetup";
        public object? Schema { get; set; }
        public object? Samples { get; set; }
        public string Prompt { get; set; } = null!;
    }

    private sealed class SuggestAiAgentApiRequest
    {
        // OperationType routes the consolidated assist endpoint (sent as the exact enum name).
        // License + CertificateThumbprint are injected by the RavenDB /assistant/assist proxy.
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

/// <summary>Draft CDC config + rationale returned by the internal cdc-config endpoint.</summary>
public sealed record SuggestCdcInternalResult(
    AiHelperStatus Status,
    CdcSinkConfiguration? Configuration,
    IReadOnlyList<string> Rationale,
    int InputTokenCount,
    int OutputTokenCount);

/// <summary>Draft agent config candidate(s) + rationale returned by the internal agent-config endpoint.</summary>
public sealed record SuggestAiAgentInternalResult(
    AiHelperStatus Status,
    IReadOnlyList<AiAgentConfiguration> Configurations,
    IReadOnlyList<string> Rationale,
    int InputTokenCount,
    int OutputTokenCount);
