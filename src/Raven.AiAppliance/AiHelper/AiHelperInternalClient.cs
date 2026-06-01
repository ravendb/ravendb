using System.Net;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Sparrow.Json;

namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Typed client for the AI-Helper endpoints on the internal AI service (api.ravendb.net).
/// Attaches the appliance license (from <see cref="IApplianceLicenseProvider"/>) and the
/// client-cert thumbprint (<c>IDocumentStore.Certificate</c>) to each request, then maps
/// transport outcomes (401/429/non-2xx) to <see cref="AiHelperStatus"/>. Request/response
/// payloads are serialized through <c>store.Conventions.Serialization</c>, keeping the wire
/// shape byte-identical to the RavenDB-based internal service.
/// Registered as a typed <c>HttpClient</c> with <c>BaseAddress</c> set to <c>ApplianceOptions.AiApiUrl</c>.
/// </summary>
public sealed class AiHelperInternalClient(
    HttpClient httpClient,
    IDocumentStore store,
    IApplianceLicenseProvider licenseProvider) : IAiHelperClient
{
    private const string CdcConfigPath = "/api/v1/ai/setup/cdc-config";
    private const string AgentConfigPath = "/api/v1/ai/setup/agent-config";

    public async Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct)
    {
        if (TryBuildAuth(out var license, out var thumbprint) == false)
            return new SuggestCdcInternalResult(AiHelperStatus.InvalidCredentials, Configuration: null, [], 0, 0);

        var request = new SuggestCdcApiRequest
        {
            License = license,
            CertificateThumbprint = thumbprint,
            Schema = schema,
            Samples = samples,
            Prompt = prompt,
        };

        var (transport, content) = await SendAsync(CdcConfigPath, request, ct);
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
        if (TryBuildAuth(out var license, out var thumbprint) == false)
            return new SuggestAiAgentInternalResult(AiHelperStatus.InvalidCredentials, [], [], 0, 0);

        var request = new SuggestAiAgentApiRequest
        {
            License = license,
            CertificateThumbprint = thumbprint,
            CdcConfig = cdcConfig,
            CollectionsSample = collectionsSample,
            Mode = mode,
            Prompt = prompt,
        };

        var (transport, content) = await SendAsync(AgentConfigPath, request, ct);
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

    /// The license is the required credential. The cert thumbprint is attached when available
    /// (null on an unsecured store) and validated by the internal service.
    private bool TryBuildAuth(out ApplianceLicense license, out string? thumbprint)
    {
        thumbprint = store.Certificate?.Thumbprint;
        return licenseProvider.TryGetLicense(out license);
    }

    private async Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, object request, CancellationToken ct)
    {
        try
        {
            using var content = new StringContent(SerializeRequest(request), Encoding.UTF8, "application/json");
            using var response = await httpClient.PostAsync(path, content, ct);
            var text = await response.Content.ReadAsStringAsync(ct);

            return response.StatusCode switch
            {
                HttpStatusCode.Unauthorized => (AiHelperStatus.InvalidCredentials, text),
                HttpStatusCode.TooManyRequests => (AiHelperStatus.OutOfTokens, text),
                _ when !response.IsSuccessStatusCode => (AiHelperStatus.InternalError, text),
                _ => (AiHelperStatus.Success, text),
            };
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            // DNS/TLS/socket failure or an HttpClient timeout (not caller cancellation) maps to
            // InternalError; caller cancellation propagates.
            // `using var response` inside the try guarantees disposal on every path.
            return (AiHelperStatus.InternalError, string.Empty);
        }
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

    private sealed class SuggestCdcApiRequest
    {
        public ApplianceLicense License { get; set; } = null!;
        public string? CertificateThumbprint { get; set; }
        public object? Schema { get; set; }
        public object? Samples { get; set; }
        public string Prompt { get; set; } = null!;
    }

    private sealed class SuggestAiAgentApiRequest
    {
        public ApplianceLicense License { get; set; } = null!;
        public string? CertificateThumbprint { get; set; }
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
