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
            if (response.StatusCode == HttpStatusCode.NotFound)
                throw new LicenseKeyNotFoundException(
                    "license API has no setup package for this key; check QUILL_LICENSE_KEY.");

            if (response.IsSuccessStatusCode == false)
                throw new LicenseRetrievalException(
                    $"license API returned {(int)response.StatusCode} {response.ReasonPhrase} retrieving the setup package.");

            await using var source = await response.Content.ReadAsStreamAsync(ct);
            await SetupPackageDownload.CopyCappedAsync(source, destination, ct);
        }
    }

    private static readonly TimeSpan RetryAfterFallback = TimeSpan.FromSeconds(30);

    private static TimeSpan RetryDelay(DelegateResult<HttpResponseMessage> outcome)
    {
        var retryAfter = outcome.Result?.Headers.RetryAfter;
        if (retryAfter is null)
            return RetryAfterFallback;

        // Retry-After is either a delta (Retry-After: 30) or an HTTP date; Delta is null for the date form.
        if (retryAfter.Delta is { } delta)
            return delta;
        if (retryAfter.Date is { } date)
            return date - DateTimeOffset.UtcNow is { Ticks: > 0 } until ? until : RetryAfterFallback;

        return RetryAfterFallback;
    }

    private static readonly AsyncRetryPolicy<HttpResponseMessage> SetupPackageRetryPolicy = Policy
        .HandleResult<HttpResponseMessage>(r => r.StatusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.TooManyRequests)
        .OrResult(r => r.StatusCode is HttpStatusCode.BadGateway or HttpStatusCode.GatewayTimeout)
        .Or<HttpRequestException>()
        .WaitAndRetryAsync(
            retryCount: 40,
            sleepDurationProvider: (_, outcome, _) => RetryDelay(outcome),
            onRetryAsync: (outcome, _, _, _) =>
            {
                // ResponseHeadersRead holds the connection until the message is disposed
                outcome.Result?.Dispose();
                return Task.CompletedTask;
            });
}
