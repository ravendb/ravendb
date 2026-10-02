using System.Text.Json.Serialization;

namespace Raven.Quill.Contracts;

public sealed record AppUsageResponse(
    AppUsageMetrics Metrics,
    SeriesData TokensByCapability,
    SeriesData TokensByModel,
    SeriesData ConversationsByChannel,
    TopCapability[] TopCapabilities);

public sealed record AppUsageMetrics(
    MetricCard Conversations,
    MetricCard Tokens,
    // Bucket start times the sparklines are indexed by (UsagePeriod.Buckets()); both cards
    // share the same buckets, so one array aligns 1:1 with each MetricCard.Sparkline.
    DateTime[] Buckets);

public sealed record MetricCard(double Value, double Delta, double[] Sparkline);

public sealed record SeriesData(Dictionary<string, object>[] Points, SeriesKey[] Keys);

public sealed record SeriesKey(string Key, string Label);

public sealed record TopCapability(string Name, long Invocations, long AvgTokens, long TotalTokens);
