namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Production <see cref="ILicenseClient"/>: fetches the setup-package zip by token from the public
/// license API (RavenDB-26783). Registered as a typed <c>HttpClient</c> with <c>BaseAddress</c> set
/// to <see cref="Hosting.ApplianceOptions.LicenseApiUrl"/>.
/// </summary>
public sealed class LicenseHttpClient(HttpClient httpClient) : ILicenseClient
{
    // RavenDB-26783: GET /api/v{version}/quill/licenses/{token} -> setup-package zip.
    // Leading-slash path replaces BaseAddress' path entirely (same idiom as AiHelperInternalClient).
    private const string LicensePathPrefix = "/api/v1/quill/licenses";

    public async Task DownloadSetupPackageToAsync(string token, Stream destination, CancellationToken ct)
    {
        var url = $"{LicensePathPrefix}/{Uri.EscapeDataString(token)}";

        HttpResponseMessage response;
        try
        {
            response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        }
        catch (HttpRequestException e)
        {
            // DNS / TLS / socket failure — the token itself is never echoed.
            throw new LicenseRetrievalException("failed to reach the license API to retrieve the setup package.")
            {
                Data = { ["inner"] = e.Message },
            };
        }

        using (response)
        {
            if (response.IsSuccessStatusCode == false)
                throw new LicenseRetrievalException(
                    $"license API returned {(int)response.StatusCode} {response.ReasonPhrase} retrieving the setup package.");

            await using var source = await response.Content.ReadAsStreamAsync(ct);
            await SetupPackageDownload.CopyCappedAsync(source, destination, ct);
        }
    }
}
