using System.Text.Json.Serialization;

namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Per-app usage analytics — the prototype's <c>getAppUsage({appId,start,end})</c>.
/// Phase 1 populates the conversation/token-derived fields; <c>cdcWrites</c>,
/// <c>topTables</c> (CDC, RavenDB-26780), <c>tokensByModel</c> (no model recorded)
/// and <c>conversationsByChannel</c> (no channel link) ship as empty skeletons —
/// see the impl handoff. <c>granularity</c> is "hour" for ranges ≤ 2 days, else "day".
/// </summary>
public sealed record AppUsageResponse(
    // Property-level converter: it outranks the global JsonStringEnumConverter in
    // Program.cs's options (which would otherwise emit PascalCase "Day").
    [property: JsonConverter(typeof(UsageGranularityConverter))] UsageGranularity Granularity,
    AppUsageMetrics Metrics,
    SeriesData TokensByCapability,
    SeriesData TokensByModel,
    SeriesData ConversationsByChannel,
    CdcWritePoint[] CdcWrites,
    TopTable[] TopTables,
    TopCapability[] TopCapabilities);

/// <summary>The three KPI cards; each carries a current value, a percent delta vs the
/// previous equal-length window, and a per-bucket sparkline.</summary>
public sealed record AppUsageMetrics(
    MetricCard Conversations,
    MetricCard Tokens,
    MetricCard CdcWrites);

public sealed record MetricCard(double Value, double Delta, double[] Sparkline);

/// <summary>A multi-series chart: <c>Points</c> is one row per time bucket shaped
/// <c>{ t, &lt;key&gt;: number, ... }</c>; <c>Keys</c> names/labels/colors each series.</summary>
public sealed record SeriesData(Dictionary<string, object>[] Points, SeriesKey[] Keys);

public sealed record SeriesKey(string Key, string Label);

public sealed record CdcWritePoint(string T, long Writes);

// TODO RavenDB-26992: LagSeconds/LastWriteAt are placeholders (0 / "") pending real per-table CDC metrics.
public sealed record TopTable(string Name, long Writes, int LagSeconds, string LastWriteAt);

public sealed record TopCapability(string Name, long Invocations, long AvgTokens, long TotalTokens);
