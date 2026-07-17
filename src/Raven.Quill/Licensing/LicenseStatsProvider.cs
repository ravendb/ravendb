using System.Text.Json;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;

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

    public async Task<QuillUsageResponse> GetUsageAsync(int year, int? month, int? day, CancellationToken token)
    {
        var r = await _ravendb.SendAsync("/admin/license/quill/usage", "POST", new
        {
            Month = month,
            Year = year,
            Day = day
        }, token);

        var usage = await _ravendb.DeserializeAsync<QuillUsageResponse>(r.Content, token);

        var perApplicationUsages = (usage.PerApplication ?? [])
            .GroupBy(p => (p.TopologyId, p.ApplicationName))
            .Select(g => new QuillApplicationUsage(
                g.Key.TopologyId,
                g.Key.ApplicationName,
                g.Min(x => x.From),
                g.Max(x => x.To),
                g.Sum(x => x.Usage)))
            .ToList();

        return new QuillUsageResponse(perApplicationUsages, usage.ByPeriod);
    }
}
