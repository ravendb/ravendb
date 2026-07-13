using System.Text.Json;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;

namespace Raven.Quill.Licensing;

internal sealed class LicenseStatsProvider : ILicenseStatsProvider
{
    private readonly IAiHelperClient _ravendb;

    public LicenseStatsProvider(IAiHelperClient ravendb)
    {
        _ravendb = ravendb;
    }

    private static readonly LicensePlan[] Plans =
    [
        new("enterprise", "Enterprise", "Production workloads", "Custom", "", false,
            ["Unlimited apps & writes", "2h SLA support"]),
    ];

    public async Task<LicenseResponse> GetLicenseAsync(CancellationToken token)
    {
        var licenseResult = await _ravendb.SendAsync("/license/status", "GET", new { }, token);
        var license = await _ravendb.DeserializeAsync<ServerLicenseResponse>(licenseResult.Content, token);

        var connectivityResult = await _ravendb.SendAsync("/license-server/connectivity", "GET", new { }, token);
        var connectivity = await _ravendb.DeserializeAsync<ConnectivityStatus>(connectivityResult.Content, token);

        return new LicenseResponse(license, connectivity, Plans);
    }

    public async Task<QuillUsageResponse> GetUsageAsync(int? year, int? month, CancellationToken token)
    {
        var usage = await _ravendb.SendAsync("/license/quill/usage", "POST", new
        {
            Month = month,
            Year = year,
        }, token);

        return await _ravendb.DeserializeAsync<QuillUsageResponse>(usage.Content, token);
    }
}
