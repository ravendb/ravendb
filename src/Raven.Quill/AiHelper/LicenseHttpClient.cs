using System.Net;
using Polly;
using Polly.Retry;
using Raven.Server.Commercial;

namespace Raven.Quill.AiHelper;

public sealed class LicenseHttpClient : ILicenseClient
{
    private const string LicensePathPrefix = "/api/v1/quill/licenses";

    public async Task DownloadSetupPackageToAsync(string licenseKey, Stream destination, CancellationToken ct)
    {
        var url = $"{LicensePathPrefix}/{Uri.EscapeDataString(licenseKey)}";

        HttpResponseMessage response;
        try
        {
            response = await ApiHttpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, SetupPackageRetryPolicy, token: ct);
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

    private static readonly AsyncRetryPolicy<HttpResponseMessage> SetupPackageRetryPolicy = Policy
        .HandleResult<HttpResponseMessage>(r => r.StatusCode is HttpStatusCode.ServiceUnavailable && r.Headers.RetryAfter != null)
        .WaitAndRetryAsync(
            retryCount: 5,
            sleepDurationProvider: (_, result, _) => result.Result.Headers.RetryAfter.Delta.Value,
            onRetryAsync: (outcome, _, _, _) =>
            {
                // ResponseHeadersRead holds the connection until the message is disposed
                using (outcome.Result)
                    return Task.CompletedTask;
            });
}
