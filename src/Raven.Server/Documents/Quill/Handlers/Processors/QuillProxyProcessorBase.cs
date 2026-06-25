using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Quill.Handlers.Processors;

internal abstract class QuillProxyProcessorBase : AbstractHandlerProcessor<RequestHandler>
{
    // Used only for the configured-upstream override path (tests / staging); the default path uses the
    // shared ApiHttpClient (api.ravendb.net). No BaseAddress - callers pass an absolute URL.
    private static readonly RavenHttpClient OverrideHttpClient = new();

    private License _license;
    private string _certificateThumbprint;

    private const string LicensePropertyName = "License";
    private const string CertificateThumbprintPropertyName = "CertificateThumbprint";

    protected QuillProxyProcessorBase([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    // Gate the proxy before forwarding. Returns false (and writes the response status) when the call
    // must not proceed, so the appliance maps it to a clean AiHelperStatus instead of a 500:
    //   - admin kill-switch (Ai.Assistant.Disable) -> 403
    //   - no server license -> 401 (matches the appliance's prior "InvalidCredentials when license absent")
    // Unlike the Studio AI assistant (AiAssistantHandlerProcessorBase), we deliberately do NOT call
    // LicenseManager.AssertCanUseAiAssistant() here: that gates a Studio-specific entitlement the
    // appliance license may not carry, and the upstream (api.ravendb.net) already validates the
    // forwarded license + quota. The kill-switch gives admins a local off-switch.
    protected bool TryAuthorize()
    {
        if (ServerStore.Configuration.Ai.DisableAiAssistant)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Forbidden;
            return false;
        }

        _license = ServerStore.LoadLicense();
        if (_license is null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
            return false;
        }

        _certificateThumbprint = RequestHandler.GetCurrentCertificate()?.Thumbprint;
        return true;
    }

    // Injects the server-held license + client-cert thumbprint into the forwarded body so the appliance
    // no longer has to carry them. Call only after TryAuthorize() returned true.
    protected void FulfillRequestMetadata(DynamicJsonValue requestBody)
    {
        requestBody[LicensePropertyName] = _license.ToJson();
        requestBody[CertificateThumbprintPropertyName] = _certificateThumbprint;
    }

    // The single forwarding seam: forward to api.ravendb.net (or a configured override) and stream the
    // response back unchanged. Future optimizations (caching, coalescing, metrics) attach here.
    protected async Task ProxyAsync(string upstreamRelativeUri, HttpContent content, CancellationToken token)
    {
        var overrideBaseUrl = ServerStore.Configuration.Ai.QuillAssistApiUrl;

        using var response = string.IsNullOrEmpty(overrideBaseUrl)
            ? await ApiHttpClient.PostAsync(upstreamRelativeUri, content, HttpCompletionOption.ResponseHeadersRead, shouldRetry: true, token).ConfigureAwait(false)
            : await OverrideHttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Post, $"{overrideBaseUrl.TrimEnd('/')}{upstreamRelativeUri}") { Content = content },
                HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);

        if (response.IsSuccessStatusCode == false)
            HttpContext.Response.StatusCode = (int)response.StatusCode;

        var contentType = response.Content.Headers.ContentType?.ToString();
        if (contentType != null)
            HttpContext.Response.Headers.ContentType = contentType;

        if (response.IsSuccessStatusCode && contentType == "text/event-stream")
            RequestHandler.DisableResponseBuffering();

        await response.Content.CopyToAsync(RequestHandler.ResponseBodyStream(), token).ConfigureAwait(false);
    }
}
