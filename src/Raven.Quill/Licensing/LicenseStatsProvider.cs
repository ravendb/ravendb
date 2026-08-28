using System.Text.Json;
using Microsoft.Extensions.Options;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;
using Raven.Quill.Metrics;

namespace Raven.Quill.Licensing;

internal sealed class LicenseStatsProvider : ILicenseStatsProvider
{
    private readonly IAiHelperClient _ravendb;
    private readonly string _configDatabase;

    public LicenseStatsProvider(IAiHelperClient ravendb, IOptions<ApplianceOptions> options)
    {
        _ravendb = ravendb;
        // The config database name is configurable (RAVEN_QUILL_CONFIG_DB), so the appliance's own
        // usage row can only be recognized by comparing against the configured value - never a literal.
        _configDatabase = options.Value.ConfigDatabase;
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
                g.Sum(x => x.Usage),
                IsSystem: string.Equals(g.Key.ApplicationName, _configDatabase, StringComparison.OrdinalIgnoreCase)))
            .ToList();

        return new QuillUsageResponse(perApplicationUsages, usage.ByPeriod);
    }
}
