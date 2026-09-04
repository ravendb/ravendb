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
        var period = new UsagePeriod(year, month, day);
        var (transport, content) = await _ravendb.SendAsync("/admin/license/quill/usage", "POST", new
        {
            Month = period.Month,
            Year = period.Year,
            Day = period.Day
        }, token);

        if (transport != AiHelperStatus.Success)
            throw new LicenseUsageUnavailableException($"the license server did not return usage data (transport {transport})");

        var usage = await _ravendb.DeserializeAsync<QuillUsageResponse>(content, token)
            ?? throw new LicenseUsageUnavailableException("the license server returned an unreadable usage response");

        // The license server reports one row per period; the UI wants one row per database.
        var perApplicationUsages = (usage.PerApplication ?? [])
            .GroupBy(p => (p.TopologyId, p.ApplicationName))
            .Select(g => new QuillApplicationUsage(
                g.Key.TopologyId,
                g.Key.ApplicationName,
                g.Min(x => x.From),
                g.Max(x => x.To),
                g.Sum(x => x.Usage)))
            .ToList();

        return new QuillUsageResponse(perApplicationUsages, FillEmptyBuckets(usage.ByPeriod ?? [], period));
    }

    // The license server only reports periods that saw writes, so the chart would otherwise skip
    // quiet hours/days. Lay the rows over the period's full bucket grid (clamped to now) so every
    // bucket is present, with zero usage where nothing was reported.
    private static List<QuillPeriodUsage> FillEmptyBuckets(List<QuillPeriodUsage> reported, UsagePeriod period)
    {
        var buckets = period.Buckets();
        var usageByBucket = new long[buckets.Count];
        foreach (var row in reported)
        {
            var i = period.IndexOf(UsagePeriod.ToUtc(row.From));
            if (i < 0)
                continue;
            usageByBucket[i] += row.Usage;
        }

        return buckets
            .Select((start, i) => new QuillPeriodUsage(start, period.BucketEnd(start), usageByBucket[i]))
            .ToList();
    }
}
