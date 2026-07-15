namespace Raven.Quill.AiHelper;

public sealed class LicenseHttpClient(HttpClient httpClient) : ILicenseClient
{
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
