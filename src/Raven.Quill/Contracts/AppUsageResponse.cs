using System.Text.Json.Serialization;

namespace Raven.Quill.Contracts;

public sealed record AppUsageResponse(
    AppUsageMetrics Metrics,
    SeriesData TokensByCapability,
    SeriesData TokensByModel,
    SeriesData ConversationsByChannel,
    CdcWritePoint[] CdcWrites,
    TopTable[] TopTables,
    TopCapability[] TopCapabilities);

public sealed record AppUsageMetrics(
    MetricCard Conversations,
    MetricCard Tokens,
    MetricCard CdcWrites);

public sealed record MetricCard(double Value, double Delta, double[] Sparkline);

public sealed record SeriesData(Dictionary<string, object>[] Points, SeriesKey[] Keys);

public sealed record SeriesKey(string Key, string Label);

public sealed record CdcWritePoint(string T, long Writes);

public sealed record TopTable(string Name, long Writes, int LagSeconds, string LastWriteAt);

public sealed record TopCapability(string Name, long Invocations, long AvgTokens, long TotalTokens);
