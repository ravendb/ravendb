using System.Text.Json;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Contracts;

namespace Raven.AiAppliance.Licensing;

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
        var result = await _ravendb.SendAsync("/license/status", "GET", new { }, token);
        var response = await _ravendb.DeserializeAsync<ServerLicenseResponse>(result.Content, token);
        return new LicenseResponse(response, Plans);
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
